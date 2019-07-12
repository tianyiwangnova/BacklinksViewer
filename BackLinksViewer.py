import pandas as pd
import datetime
import requests
from datetime import datetime, timedelta
from py2neo import Graph

class BackLinksViewer:
    '''
    EXAMPLE INPUT:

    config = {
    
    #only pick 3 domains for now
    'domains': ['draftkings.com', 'fanduel.com', 'williamhill.com'],
    
    'days_offset': 2,
    
    'api_args': {
        
        'majestic' : {'key': '*****',
                      'base_link': 'https://api.majestic.com/api/json',
                      'params': {'cmd' : 'GetBackLinkData',
                               'datasource' : 'fresh',
                               'Count' : 50000},
                      'columns_keep': ['SourceURL',
                                  'TargetURL',
                                  'AnchorText',
                                  'SourceTrustFlow',
                                  'SourceCitationFlow',
                                  'SourceTopicalTrustFlow_Topic_0',
                                  'TargetTopicalTrustFlow_Topic_0',
                                  'LastSeenDate']},
        
        'neo4j': {'uri': '"bolt://localhost:7687"',
                    'user_name': "neo4j",
                    'password': '*****',
                    }
    }
}
    '''

    def __init__(self, config):
        self.domains = config['domains']
        self.api_args = config['api_args']
        self.results = {}
        self.today = (datetime.today() - timedelta(days=config['days_offset'])).strftime("%Y-%m-%d")

    @staticmethod
    def compose_query_link(params):
        '''
        paste arguments together with "&" to form the query pargt which will be added to the base link
        '''
        link = ""
        for p in params:
            arg = "&{}={}".format(p, params[p])
            link += arg
        return link
    
    @staticmethod
    def link_clean(link):
        '''
        "https://www.google.com/" --> "google.com"
        '''
        try:
            prefs = ['https://www.','http://www.', 'www.', 'http://','https://']
            for p in prefs:
                if link.startswith(p):
                    result = link.split(p)[1]
                    break
                else:
                    result = link

            if link.endswith('/'):
                result = result[:-1]

            return result
        except:
            return ''

    def link_clean_for_df(self, df):
        link_columns = list(df.columns[df.columns.str.startswith("linking")])
        for l in link_columns:
            df[l] = df[l].apply(self.link_clean)
        return df

    def get_data_majestic(self, max_results = None, domains = None, write_to_class = True):
        '''
        Download backlinks data from Majestic (https://developer-support.majestic.com/api/commands/get-back-link-data.shtml)
        Only keep the links that are still seen (no more than 3 days before today)
        Maximum results return allowed: 50,000
        '''
        begin = datetime.now() 

        majestic_args = self.api_args['majestic']
        key = majestic_args['key']
        base_link = majestic_args['base_link']
        params = majestic_args['params'].copy()
        columns_keep = majestic_args['columns_keep']
        if domains is None:
            domains = self.domains

        if max_results is not None:
            params['Count'] = max_results

        print(params)

        combined_pd = pd.DataFrame()

        if 'LastSeenDate' not in columns_keep:
            columns_keep.append('LastSeenDate')
        
        for d in domains:
            print("After {} min, at domain {}".format(round((datetime.now() - begin).seconds / 60, 2), d))
            try:
                params['item'] = d
                query_link = base_link + "?app_api_key={}".format(key) + self.compose_query_link(params)
                result = requests.get(query_link)
                result_pd = pd.DataFrame.from_records(result.json()['DataTables']['BackLinks']['Data'])[columns_keep].rename({'SourceURL':'linkingFrom'}, axis=1)
                result_pd = result_pd[result_pd['LastSeenDate'] >= self.today]
                result_pd['linkingToDomain'] = params['item']
                combined_pd = pd.concat([combined_pd, result_pd])
            except:
                pass
        
        combined_pd = combined_pd[combined_pd['linkingFrom'].notna()][combined_pd['linkingToDomain'].notna()]
        combined_pd['linkingFromDomain'] = combined_pd['linkingFrom'].apply(lambda x : x.split('/')[2])
        combined_pd = self.link_clean_for_df(combined_pd)

        try:
            combined_pd['TargetURL'] = combined_pd['TargetURL'].apply(lambda x: '/'.join(x.split('/')[3:]))
        except:
            pass

        try:
            combined_pd['Topic'] = combined_pd['SourceTopicalTrustFlow_Topic_0'].apply(lambda x: x.replace('/','_').replace(' ','_').replace('-','_'))
        except:
            pass

        time_pass = round((datetime.now() - begin).seconds / 60, 2)
        print("[BackLinksViewer] [get_data_majestic] took {} min".format(time_pass))
        
        if write_to_class:
           self.results['majestic'] = combined_pd

        return combined_pd

    def neo4j_viz_one_domain(self, domain, df=None):
        '''
        Visualize backlinks for one domain;
        Include the links on the domain and backlinks (on both domain level and link level)
        The logic of the graph is:
             our_domain <-- links_on_our_domain <-- backlinks_to_the_links <-- domains_of_the_backlinks
        '''
        neo_args = self.api_args['neo4j']
        graph = Graph(uri=neo_args['uri'], auth=(neo_args['user_name'], neo_args['password']))
        graph.delete_all()
        if df is None:
            df = self.results['majestic']
        df = df[df["linkingToDomain"] == domain]
        
        graph.run("create (d:Domain{url:'" + domain + "'})")

        #Add links to the domain
        for link in df['TargetURL'].unique():
            graph.run("match (d:Domain{url:'" +
                      domain +
                      "'}) create (l:Link" + 
                      "{url:'"+ 
                      link +
                      "'})-[:Is_the_Link_of]->(d)")

        #Add backlinks domains (bd)
        for backlink_domain in df['linkingFromDomain'].unique():
            graph.run("create (bd:Backlinks_Domain" + 
                      "{url:'"+ 
                      backlink_domain 
                      +"'})")

        #Add backlinks (bl)
        for index, row in df.iterrows():
            graph.run("match (bd:Backlinks_Domain{url:'" +
                          row['linkingFromDomain'] +
                          "'}) create (bl:BackLinks_" + 
                          row['Topic'] + 
                          "{url:'"+ 
                          row['linkingFrom'] +
                          "'})-[:Links_Of_This_Domain]->(bd)")

        #Add link referral relationships
        for index, row in df.iterrows():
            graph.run("match (bl:BackLinks_" +
                          row['Topic'] +
                          "{url:'"+ 
                          row['linkingFrom'] +
                          "'}), (l:Link{url:'" +
                          row['TargetURL'] +
                          "'}) create (bl)-[:Refers]->(l)")
        print("please go to your Neo4j browser and run `match (n) return n` and display the graph")

    def neo_viz_multiple_links_level(self, domains, df=None):
        '''
        Take neo4j_viz_one_domain to multiple domains so that we can compare and see overlapped backlinks between multiple domains
        '''
        neo_args = self.api_args['neo4j']
        graph = Graph(uri=neo_args['uri'], auth=(neo_args['user_name'], neo_args['password']))
        graph.delete_all()
        if df is None:
            df = self.results['majestic']
        df = df[df["linkingToDomain"].isin(domains)]

        for d in domains:
            graph.run("create (d:Domain{url:'" + d + "'})")
        
        #Add backlinks domains (bd)
        for backlink_domain in df['linkingFromDomain'].unique():
            graph.run("create (bd:Backlinks_Domain" + 
                          "{url:'"+ 
                          backlink_domain 
                          +"'})")

        #Add backlinks (bl)
        for index, row in df.iterrows():
            graph.run("match (bd:Backlinks_Domain{url:'" +
                          row['linkingFromDomain'] +
                          "'}) create (bl:BackLinks_" + 
                          row['Topic'] + 
                          "{url:'"+ 
                          row['linkingFrom'] 
                          +"'})-[:Links_Of_This_Domain]->(bd)")

        #Add link referral relationships
        for index, row in df.iterrows():
            graph.run("match (bl:BackLinks_" +
                          row['Topic'] +
                          "{url:'"+ 
                          row['linkingFrom'] +
                          "'}), (d:Domain{url:'" +
                          row['linkingToDomain'] +
                          "'}) create (bl)-[:Refers]->(d)")

        print("please go to your Neo4j browser and run `match (n) return n` and display the graph")

    def neo_viz_multiple_domains_level(self, domains, df=None):
        '''
        Take neo4j_viz_one_domain to multiple domains so that we can compare and see overlapped backlinks between multiple domains;
        Only visualize the domains
        '''
        neo_args = self.api_args['neo4j']
        graph = Graph(uri=neo_args['uri'], auth=(neo_args['user_name'], neo_args['password']))
        graph.delete_all()
        if df is None:
            df = self.results['majestic']
        df = df[df["linkingToDomain"].isin(domains)]

        for d in domains:
            graph.run("create (d:Domain{url:'" + d + "'})")
        #Add backlinks domains (bd)
        for backlink_domain in df['linkingFromDomain'].unique():
            graph.run("create (bd:Backlinks_Domain" + 
                          "{url:'"+ 
                          backlink_domain +
                          "'})")

        #Add link referral relationships
        for index, row in df.iterrows():
            graph.run("match (bd:Backlinks_Domain" +
                          "{url:'"+ 
                          row['linkingFromDomain'] +
                          "'}), (d:Domain{url:'" +
                          row['linkingToDomain'] +
                          "'}) create (bd)-[:Refers]->(d)"     
                          )
        print("please go to your Neo4j browser and run `match (n) return n` and display the graph")
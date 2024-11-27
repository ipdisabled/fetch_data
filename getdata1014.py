# encoding:utf-8
'''
bak_foot_his:
https://caipiao.eastmoney.com/Result/History/sfc?page=1
https://www.lottery.gov.cn/kj/kjlb.html?sfc

bak_foot_train / bak_foot_new:
(https://cp.zgzcw.com/lottery/getissue.action?lotteryId=300&issueLen=20
https://cp.zgzcw.com/lottery/zcplayvs.action?lotteryId=13&issue=
https://fenxi.zgzcw.com/?playid?/bjop)

(https://webapi.sporttery.cn/gateway/lottery/getFootBallMatchV1.qry?param=90,0&sellStatus=0&termLimits=10
https://www.sporttery.cn/jc/zqdz/index.html?showType=2&mid=1027293)

foot_train / foot_new:
https://live.500star.com/zucai.php?e=24168
'''
import os
import re
import time
import chardet
import logging
import requests
import pandas as pd
from lxml import etree
from datetime import datetime
from urllib.parse import urlparse
from collections import deque,defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def flatten_dict(multi_li,columns,rows):
    return {f"{col},{row}": multi_li[row_idx][col_idx]
                for row_idx,row in enumerate(rows)
                for col_idx,col in enumerate(columns)}

def ftext(element):
    try:
        return ''.join(element.itertext()).strip() \
        if element is not None else ''
    except Exception as e:
        return ''

def get_local_df(path):
    if path and os.path.isfile(path):
        return pd.read_csv(path)

def save_data(in_data,path='',colname=[]):
    if isinstance(in_data,list) and colname and path:
        df = pd.DataFrame(in_data,columns=colname)
        df.to_csv(path,index=False)
    elif isinstance(in_data,pd.DataFrame):
        in_data.to_csv(path,index=False)

pipeline = {
    'dlt':{
        'nodes':[
            {'id':'dlt','func':'fetch_parse_a',
             'url':'https://data.17500.cn/dlt_asc.txt',
             'save':{'path':'dlt_data.csv','colname':['index','time','r1','r2','r3','r4','r5','b1','b2']}}
        ],
        'links':[]
    },
    'ssq':{
        'nodes':[
            {'id':'ssq','func':'fetch_parse_a',
             'url':'https://data.17500.cn/ssq_asc.txt',
             'save':{'path':'ssq_data.csv','colname':['index','time','r1','r2','r3','r4','r5','r6','b1']}}
        ],
        'links':[]     
    },
    'kl8':{
        'nodes':[
            {'id':'kl8','func':'fetch_parse_a',
             'url':'https://data.17500.cn/kl8_asc.txt',
             'save':{'path':'kl8_data.csv','colname':['index','time','r1','r2','r3','r4','r5','r6','r7',
             'r8','r9','r10','r11','r12','r13','r14','r15','r16','r17','r18','r19','r20']}}
        ],
        'links':[]     
    },
    'pl3':{
        'nodes':[
            {'id':'pl3','func':'fetch_parse_a',
             'url':'https://data.17500.cn/pl3_asc.txt',
             'save':{'path':'pl3_data.csv','colname':['index','time','r1','r2','r3']}}
        ],
        'links':[]    
    },
    'pl5':{
        'nodes':[
            {'id':'pl5','func':'fetch_parse_a',
             'url':'https://data.17500.cn/pl5_asc.txt',
             'save':{'path':'pl5_data.csv','colname':['index','time','r1','r2','r3','r4','r5']}}
        ],
        'links':[]
    },
    'foot_new':{
        'nodes':[
            {'id':'match_li','func':'fetch_parse_c',
             'url':'https://live.500star.com/zucai.php/',
             'save':{'path':'next_match.csv'}},
            {'id':'odds_li','func':'fetch_parse_d',
             'save':{'path':'next_match_odds.csv'}},
            {'id':'sj_li','func':'fetch_parse_e',
             'save':{'path':'next_match_sj.csv','colname':
              ['playid','team','host','m0_cnt','w0','d0','l0','m0_hit','m0_mis',
               'm1_cnt','w1','d1','l1','m1_hit','m1_mis','m_dval','m_num','m_r','m_p']}}
        ],
        'links':[{'from':'match_li','to':'odds_li'},
                 {'from':'match_li','to':'sj_li'}]
    },
    'foot_his':{
        'nodes':[
            {'id':'foot_his','func':'fetch_parse_b',
             'url':'https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?gameNo=90&provinceId=0&&isVerify=1&pageSize=30&pageNo=',  
             'save':{'path':'foot_his_result.csv','colname':['index','time','r1','r2','r3','r4','r5','r6','r7',
             'r8','r9','r10','r11','r12','r13','r14']}}
        ],
        'links':[]
    },
    'foot_train':{
        'nodes':[
            {'id':'playinfo_li','func':'fetch_parse_cc',
             'url':'https://live.500star.com/zucai.php/?e=',
             'input':['foot_his_result.csv'],
             'save':{'path':'foot_play_info.csv'}},
            {'id':'odds_li','func':'fetch_parse_d','extend':1,
             'save':{'path':'train_match_odds.csv'}},
            {'id':'sj_li','func':'fetch_parse_e','extend':1,
             'save':{'path':'train_match_sj.csv','colname':
              ['playid','team','host','m0_cnt','w0','d0','l0','m0_hit','m0_mis',
               'm1_cnt','w1','d1','l1','m1_hit','m1_mis','m_dval','m_num','m_r','m_p']}}
        ],
        'links':[{'from':'playinfo_li','to':'odds_li'},
                 {'from':'playinfo_li','to':'sj_li'}]
    }
}

class Fetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
            'Accept':'application/json,text/html,*/*;q=0.9',
            'Accept-Language':'en-us,en;q=0.9',
            'Connection': 'keep-alive'
        })

    def fetch_url(self,url,rep_t,sleeptime:int=4,timeout:int=10,retry:int=2):
        parsed_url = urlparse(url)
        self.session.headers.update({'Referer':f"{parsed_url.scheme}://{parsed_url.netloc}"})
        for attempt in range(retry):
            try:
                logger.info(f"Fetching URL:{url}")
                response = self.session.get(url,timeout=timeout)
                time.sleep(sleeptime)
                response.encoding = chardet.detect(response.content)['encoding']
                response.raise_for_status()
                if rep_t =='json':
                    return response.json()
                elif rep_t =='text':
                    return response.text if response.text else None
                elif rep_t =='html':
                    return etree.HTML(response.text) if response.text else None
            except Exception as e:
                logger.error(f"{e}：{url}")
                if attempt < retry-1:
                    logger.info("Retrying...")
                else:
                    return None

class SNode:
    def __init__(self,id,func,url='',extend=0,input=None,output=None,save=None):
        self.id = id
        self.url = url
        self.pfunc = func
        self.extend = extend if extend is not None else 0
        self.input = input if input is not None else []
        self.output = output if output is not None else []
        self.path = save.get('path') if save else None
        self.colname =  save.get('colname') if save else None
    
    def execute(self):
        func = getattr(self,self.pfunc)
        func(self.input,self.url)

    def fetch_parse_a(self,input,url):
        '''get input +combine url'''
        data = fetcher.fetch_url(url,'text')
        if data:
            str_li = data.strip().split('\n')
            for str_line in str_li:
                sp_li = str_line.split()[:len(self.colname)]
                self.output.append(sp_li)
            save_data(self.output,self.path,self.colname)

    def fetch_parse_b(self,input,baseurl):
        local_df = get_local_df(self.path)
        local_li = local_df.values.tolist() if local_df is not None else []
        localnum = int(local_df.iloc[0,0]) if local_df is not None else 0
        need_update,numpage = True,1
        while need_update:
            data = fetcher.fetch_url(f'{baseurl}{numpage}','json')
            numpage += 1
            if not data or numpage > data['value']['pages']:
                break                  
            for resitem in data['value']['list']:
                result_tup = tuple(int(float(num)) if num != '*' else num \
                            for num in resitem['lotteryDrawResult'].split())  
                data_string = (resitem['lotteryDrawNum'],resitem['lotteryDrawTime']) \
                              + result_tup
                if int(data_string[0]) > localnum:
                    self.output.append(data_string)
                else:
                    need_update =False
                    break
        self.output.extend(local_li)
        save_data(self.output,self.path,self.colname)

    def fetch_parse_c(self,input,url):
        dtree = fetcher.fetch_url(url,'html')
        if dtree is not None:
            playid = dtree.xpath('//select[@id="sel_expect"]/option/@value')[0]
            row_status = dtree.xpath('//tr[@status="0"]')
            for row in row_status:
                td_elements = row.xpath('td[6]|td[8]|td[10]|td[11]')

                host = re.sub(r'\[\d+\](\d+)?','',ftext(td_elements[0]))
                guest = re.sub(r'\d*\[\d+\]','',ftext(td_elements[1]))
                links = td_elements[2].xpath('.//a[contains(text(),"析") or contains(text(),"欧")]') \
                        if len(td_elements) > 2 else []
                link1,link2 = (f'https:{link.get("href")}' for link in links[:2]) \
                              if len(links)>1 else ('','')

                result = ftext(td_elements[3]).split(' ')[0]
                self.output.append({'playid':playid,'host':host,'guest':guest,\
                                    'result':result,'sj':link1,'odds':link2})
        output_df = pd.DataFrame(self.output)
        save_data(output_df,self.path)
        del output_df

    def fetch_parse_cc(self,input,baseurl):
        new_hisdf = get_local_df(input[0])
        local_playinfo = get_local_df(self.path)
        local_playid = int(local_playinfo['playid'][0]) \
                    if local_playinfo is not None else 0
        new_hisdf['time'] = pd.to_datetime(new_hisdf['time'])
        one_year_ago = datetime(datetime.now().year - 1, 1, 1)

        update_playid = new_hisdf[(new_hisdf['index']>local_playid) &
                                  (new_hisdf['time'] >= one_year_ago)]['index'].tolist()
        for playid in update_playid:
            dtree = fetcher.fetch_url(f'{baseurl}{playid}','html')
            if dtree is not None:
                row_status = dtree.xpath('//tr[@status="4"]')
                for row in row_status:
                    td_elements = row.xpath('td[6]|td[8]|td[10]|td[11]')

                    host = re.sub(r'\[\d+\](\d+)?','',ftext(td_elements[0]))
                    guest = re.sub(r'\d*\[\d+\]','',ftext(td_elements[1]))
                    links = td_elements[2].xpath('.//a[contains(text(),"析") or contains(text(),"欧")]') \
                            if len(td_elements) > 2 else []
                    link1,link2 = (f'https:{link.get("href")}' for link in links[:2]) \
                                if len(links)>1 else ('','')
                    result = ftext(td_elements[3])
                    self.output.append({'playid':playid,'host':host,'guest':guest,\
                                        'result':result,'sj':link1,'odds':link2})
        output_df = pd.DataFrame(self.output)
        local_playinfo = pd.concat([output_df,local_playinfo],ignore_index=True)
        save_data(local_playinfo,self.path)
        del output_df,local_playinfo

    def fetch_parse_d(self,input,url):
        columns = ('old_odw','old_odd','old_odl','new_odw','new_odd','new_odl',\
                   'old_wp','old_dp','old_lp','new_wp','new_dp','new_lp')
        targets = [('最低值','平均值','最高值'),('min','mean','max')]

        odds_dict_li,local_odds_df = [],None
        if self.extend != 0:
            local_odds_df = get_local_df(self.path)
            unique_playids = local_odds_df['playid'].drop_duplicates().head(365)
            local_odds_df = local_odds_df[local_odds_df['playid'].isin(unique_playids)]
        max_play_id = local_odds_df.iloc[0]['playid']if local_odds_df is not None else 0
        update_match_li = [item for item in input if int(item['playid']) > max_play_id]

        for match in update_match_li:
            oddstree = fetcher.fetch_url(match['odds'],'html')
            if oddstree is not None:
                results = [[] for _ in range(len(targets[0]))]
                for i,target in enumerate(targets[0]):
                    target_td = oddstree.xpath(f"//td[text()='{target}']")
                    if target_td:
                        siblings = target_td[0].getnext(),target_td[0].getnext().getnext()
                        for sibling in siblings:
                            if sibling is not None:
                                td_elements = sibling.xpath('.//td')
                                results[i].extend(ftext(td) for td in td_elements)
                odds_dict = {}
                odds_dict.update({'playid':match['playid'],'host':match['host'],
                                  'guest':match['guest'],'result':match['result']})
                odds_dict.update(flatten_dict(results,columns,targets[1]))
                odds_dict_li.append(odds_dict)
                del results,odds_dict

        next_odds_df = pd.DataFrame(odds_dict_li)
        if not next_odds_df.empty and local_odds_df is not None :
            next_odds_df = pd.concat([next_odds_df, local_odds_df], ignore_index=True,copy=False)
        elif next_odds_df.empty:
            next_odds_df = local_odds_df
        save_data(next_odds_df,self.path)
        del update_match_li,odds_dict_li,next_odds_df,local_odds_df

    def fetch_parse_e(self,input,url):

        local_sj_df =None
        if self.extend != 0:
            local_sj_df = get_local_df(self.path)
            unique_playids = local_sj_df['playid'].drop_duplicates().head(365)
            local_sj_df = local_sj_df[local_sj_df['playid'].isin(unique_playids)]
            #self.output = local_sj_df.values.tolist() if local_sj_df is not None else []
        max_play_id = local_sj_df.iloc[0]['playid']if local_sj_df is not None else 0
        update_match_li = [item for item in input if int(item['playid']) > max_play_id]

        for match in update_match_li:
            sjtree = fetcher.fetch_url(match['sj'],'html')
            if sjtree is not None:
                chenji_td = sjtree.xpath('//td[@class="td_one" and text()="总成绩"]/following-sibling::td')
                chenji_li = [tuple() for _ in range(2)]
                half_len = len(chenji_td) // 2
                for idx,td in enumerate(chenji_td):
                    chenji_li[idx < half_len] +=(ftext(td),)

                zhanj_p = sjtree.xpath('//p[contains(text(),"近10场战绩")]')
                zhanji_li = []
                for i in range(min(len(zhanj_p),2)):
                    search_r = re.search(r'(\w+)近(\d+)场战绩(\d+)胜(\d+)平(\d+)负进(\d+)球失(\d+)球',ftext(zhanj_p[i]))
                    if search_r:
                        ishost = 1 if i ==0 else 0
                        zhanji_li.append((
                            match['playid'],search_r.group(1),ishost,
                            int(search_r.group(2)),int(search_r.group(3)),
                            int(search_r.group(4)),int(search_r.group(5)),
                            int(search_r.group(6)),int(search_r.group(7)),
                        ))
            self.output.extend(zhanji +chenji for zhanji,chenji in zip(zhanji_li,chenji_li))
        
        next_sj_df = pd.DataFrame(self.output,columns=self.colname)
        if not next_sj_df.empty and local_sj_df is not None :
            next_sj_df = pd.concat([next_sj_df, local_sj_df], ignore_index=True,copy=False)
        elif next_sj_df.empty:
            next_sj_df = local_sj_df
        save_data(next_sj_df,self.path)
        del update_match_li,next_sj_df,local_sj_df

    def fetch_parse_f(self,input,baseurl):
        pass

class Graph:
    def __init__(self):
        self.graph = defaultdict(list)
        self.indegree = defaultdict(int)

    def add_link(self,u:SNode,v:SNode):
        self.graph[u].append(v)
        self.indegree[v] += 1
        self.indegree.setdefault(u,0)

    def bfs(self):
        queue = deque(node for node in self.indegree if self.indegree[node] == 0)
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)
            node.execute()
            for neighbor in self.graph[node]:
                neighbor.input.extend(output for output in node.output if output not in neighbor.input)
                self.indegree[neighbor] -= 1
                if self.indegree[neighbor] == 0:
                    queue.append(neighbor)
        return result

def init_fetcher():
    global fetcher
    fetcher = Fetcher()

def load_graph_from_config(config):
    g = Graph()
    nodes = {node_config['id']:SNode(**node_config) 
                for node_config in config['nodes']}
    for node in nodes.values():
        g.indegree[node]
    for link in config['links']:
        g.add_link(nodes[link['from']],nodes[link['to']])
    for node in g.indegree.keys():
        node.output.clear()
    return g

if __name__ == "__main__":
    init_fetcher()
    for key,value in pipeline.items():
        g = load_graph_from_config(value)
        if len(g.indegree) == 1:
            single_node = next(iter(g.indegree))
            single_node.execute()
            #logger.info(f'Output:{single_node.output}')
            logger.info(f'Node Id:{single_node.id}')
        else:
            result = g.bfs()
            for node in result:
                #logger.info(f'Input:{node.input},Output:{node.output}')
                logger.info(f'Node Id:{node.id}')
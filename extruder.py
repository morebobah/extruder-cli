"""
Autor: Vladimir Mankus
Company: Megafon
This CLI utility help dowloads data from unified project management system (ESUP) made in asynchronous style.
"""

import os
import re
import zlib
import json
import time
import typer
import base64
import pickle
import asyncio
import datetime
import requests
import pandas as pd
from aiohttp import web
from typing_extensions import Annotated
from requests_negotiate_sspi import HttpNegotiateAuth

app = typer.Typer(help='Application for easiest get datas from esup.')

class http:
    """
    Just requests to server.
    Just because requests are not asincro and must be lunched in other thread.
    And all this because aiohttp not supported httpnegotiateauth.
    And also namespaces are one honking great idea -- let's do more of those!
    """

    def __init__(self, headers: dict) -> None:
        self.auth = HttpNegotiateAuth()
        self.headers = self.do_main_headers(headers)

    def do_main_headers(self, extra: dict) -> dict:
        """Create main headers"""

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36 Edg/97.0.1072.55",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            "Connection": "keep-alive",
            "Accept": "*/*"
        }
        headers.update(extra)
        return headers
    
    def get(self, url: str) -> object:
        """Get request in separate thread"""
        return requests.get(url=url, auth=self.auth, headers=self.headers)
    
    def post(self, url: str, data: str) -> object:
        """Post request in separate thread"""

        self.headers.update({"Content-Length": str(len(data))})
        return requests.post(url=url, data=data, auth=self.auth, headers=self.headers)

class extruder:
    """
    One object of extruder for one activitie in esup.
    Class for storing identifiers and methods for their preparation.
    """

    def __init__(self, activityId: int) -> None:
        """Init class with esupid."""

        if isinstance(activityId, int):
            activityId = str(activityId)
        
        self.activityId = activityId
        self.TemplateId = None
        self.ViewId = None
        self.TaskId = None
        self.TaskContent = dict()
        self.subscribers = list()
        self.hidden_url = os.environ.get('ESUPPATH')
    
    async def status(self, value: int) -> bool:
        """Check requests status"""

        if 199 < value < 400:
            return False
        return True
    
    async def addsubscriber(self, subscriber: object) -> None:
        """Add new subscriber"""

        if hasattr(self, 'subscribers'):
            self.subscribers = list()

        self.subscribers.append(subscriber)

    async def event(self, e: dict) -> dict:
        """Do events in all subsribers and return the same dict"""

        for subscriber in self.subscribers:
            await subscriber.event(e)

        return e

        
    async def FindByActivityId(self) -> dict:
        """Find all elements with a given id"""

        if not (self.TemplateId is None or self.ViewId is None):
            return await self.event({'func': 'FindByActivityId', 'status': True, 'id': self.activityId})
        
        headers = {"Content-Type": "application/json, text/javascript, */*; q=0.01"}
        url = f'http://{self.hidden_url}/Activities/FindByActivityId'
        data = str({'activityId': self.activityId})
        
        r = await asyncio.to_thread(http(headers).post, url, data)

        if await self.status(r.status_code):
            return await self.event({'func': 'FindByActivityId', 'status': False, 'id': self.activityId})
    
        result = r.json()
        self.TemplateId = result.get('TemplateId', None)
        self.ViewId = result.get('ViewId', None)

        if self.TemplateId is None or self.ViewId is None:
            return await self.event({'func': 'FindByActivityId', 'status': False, 'id': self.activityId})
        
        return await self.event({'func': 'FindByActivityId', 'status': True, 'id': self.activityId})
    
    async def GridRead(self, taskName: str) -> dict:
        """Method for get all tasks from one item"""

        if self.TaskId is not None:
            return await self.event({'func': 'GridRead', 'status': True, 'id': self.activityId})
        
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

        url = f'http://{self.hidden_url}/Activities/Grid_Read'

        data = "sort=&page=1&pageSize=1&group=&filter=ID~gt~'"
        data += '{"attributeType":"LookUp","mode":0,"value":[{"operator":"IsContainedIn","value":"' + str(self.activityId) + '"}]}'
        data += "'"
        data += '&viewId=' + str(self.ViewId) + '&myViewId=&templateId=' + str(self.TemplateId) + '&workObjectId='
        
        r = await asyncio.to_thread(http(headers).post, url, data)

        if await self.status(r.status_code):
            return await self.event({'func': 'GridRead', 'status': False, 'id': self.activityId})
    
        result = r.json()

        for key, val in result["Data"][0].items():
            if key[:8] == 'TaskList':
                if val["Name"]==taskName:
                    self.TaskId = val["ID"]
                    return await self.event({'func': 'GridRead', 'status': True, 'id': self.activityId})

        return await self.event({'func': 'GridRead', 'status': False, 'id': self.activityId})
    
    async def EsupTask(self) -> dict:
        """Loads task page by TaskId"""

        if self.TaskId is None:
            return {'func': 'EsupTask', 'status': False, 'id': self.activityId}
        
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        url = f'http://{self.hidden_url}/EsupTask?taskId=' + str(self.TaskId)

        r = await asyncio.to_thread(http(headers).get, url)

        if await self.status(r.status_code):
            return await self.event({'func': 'EsupTask', 'status': False, 'id': self.activityId})
        
        html = r.content.decode('utf-8')

        p = re.compile(r'kendo.observable\({\ *Item\ *:\ *({.*})}\);')
        m = p.search(html)
        if m:
            self.TaskContent = json.loads(m.group(1))
            return await self.event({'func': 'EsupTask', 'status': True, 'id': self.activityId})
        
        return await self.event({'func': 'EsupTask', 'status': False, 'id': self.activityId})
    
    async def pipeline(self, line: list) -> dict:
        """Prepare data via pipeline"""
        result = {'func': 'pipeline', 'status': False}

        for k, v in line.items():
            method = getattr(self, k, None)
            if method:
                if v:
                    result = await method(v)
                else:
                    result = await method()

                if isinstance(result, dict):

                    if not result['status']:
                        return await self.event(result)

                else:
                    return await self.event({'func': 'pipeline', 'status': False})
            else:
                return await self.event({'func': 'pipeline', 'status': False})

        return await self.event(result)
        
    async def getValue(self, parametr_name: str) -> dict:
        """Get value by name from loaded context"""
        if not isinstance(self.TaskContent, dict):
            return await self.event({'func': 'getValue', 'status': False, 'id': self.activityId})

        if self.TaskContent.get(parametr_name, False):
            return await self.event({'func': 'getValue', 'status': True, 'value': [self.activityId, self.TaskContent[parametr_name]], 'id': self.activityId})
        
        for val in self.TaskContent['Parameters']:
            if val["Name"] == parametr_name:
                return await self.event({'func': 'getValue', 'status': True, 'value': [self.activityId, val["Value"]], 'id': self.activityId})
        
        return await self.event({'func': 'getValue', 'status': False, 'id': self.activityId})
    
    async def setValue(self, parametr_name: str, newValue: object) -> dict:
        """Set value by name to loaded context"""

        if not isinstance(self.TaskContent, dict):
            return await self.event({'func': 'setValue', 'status': False, 'id': self.activityId})
        
        if self.TaskContent.get(parametr_name, False):
            self.TaskContent[parametr_name] = newValue
        
        for val in self.TaskContent['Parameters']:
            if val["Name"] == parametr_name:
                val["Value"] = newValue
        
        return await self.event({'func': 'setValue', 'status': False, 'id': self.activityId})
    
    def clear(self) -> None:
        """Clear rubish for pickle"""
        self.TaskContent = None
        self.subscribers = list()
    
    def __hash__(self) -> int:
        """ActivityID must be unique"""
        return self.activityId
    
class edate:
    """Class for handling esup dates, it represent in timestamp format."""

    def __init__(self, timestamp: any) -> None:
        self.ts = None

        if isinstance(timestamp, str):
            
            if timestamp.isdigit():
                self.ts = int(timestamp)
            else:
                p = re.compile(r'/Date\(([0-9]{13})\)/')
                m = p.search(timestamp)
                if m:
                    timestamp = m.group(1)
                    self.ts = int(timestamp)

        if isinstance(timestamp, int):
            self.ts = timestamp
    
    def __eq__(self, other) -> bool:
        if isinstance(other, str):
            other = edate(other)
        return (self.ts == other.ts)
    
    def __ne__(self, other) -> bool:
        if isinstance(other, str):
            other = edate(other)
        return (self.ts != other.ts)
    
    def __lt__(self, other) -> bool:
        if isinstance(other, str):
            other = edate(other)
        return (self.ts < other.ts)
     
    def __le__(self, other) -> bool:
        if isinstance(other, str):
            other = edate(other)
        return (self.ts <= other.ts)
       
    def __gt__(self, other) -> bool:
        if isinstance(other, str):
            other = edate(other)
        return (self.ts > other.ts)
    
    def __ge__(self, other) -> bool:
        if isinstance(other, str):
            other = edate(other)
        return (self.ts >= other.ts)
    
    def datetime(self) -> datetime:
        return datetime.datetime.fromtimestamp(self.ts/1000) if not self.ts is None else datetime.datetime(2000, 1, 1)
        
    def timestamp(self) -> str:
        return (f'/Date({self.ts})/')


class webmethods:
    """Implements web interface when cli options so hard"""

    def __init__(self, data):
        
        self.html = zlib.decompress(base64.b64decode(data)).decode()
        #with open('D:/source/tst01/fromfile.html', 'r') as f:
        #    self.html = f.read()
        self.mainstream = list()
    
    async def event(self, val: dict) -> dict:
        """Append event message to list"""

        self.mainstream.append(val)

    async def index(self, request: object) -> None:
        """Main page. Offers to select a file to upload"""

        return web.Response(text = self.html, content_type='text/html')

    async def fileupload(self, request: object) -> None:
        """Main page. Show sheets of excel file, offers to select one."""

        dataset = request.rel_url.query['dataset'].replace("\\", "/")
        try:
            xl = pd.ExcelFile(dataset)
            context = ''
            for idx in range(len(xl.sheet_names)):
                context += f'<div class="xl-sheets" onclick="javascript: window.location.replace(\'/tableopen?dataset={dataset}&sheet={idx}\')">{xl.sheet_names[idx]}</div>'
            self.sheets = context
            
        except FileNotFoundError:
            context = f'No such file or directory: "{dataset}". Try again!'

        html = self.html.replace('<!--table-->', context, 1)
        return web.Response(text = html, content_type='text/html')

    async def tableopen(self, request: object) -> None:
        dataset = request.rel_url.query['dataset']
        sheet_num = request.rel_url.query['sheet']

        if sheet_num.isdigit():
            try:
                xl = pd.ExcelFile(dataset)
                df = xl.parse(xl.sheet_names[int(sheet_num)])
                context = '<table><tr>'
                for idx, col in enumerate(df.columns):
                    context += f'<th class="xl-column" onclick="setcol(event, {idx})" id="col{idx}">{col}</th>'
                for rows in df.values[:5]:
                    context += '</tr><tr>'
                    for row in rows:
                        context += f'<td>{row}</td>'        
                context += '</tr>'
                context += '</table>'
                self.table = context

            except FileNotFoundError:
                context = f'No such file or directory: "{dataset}". Try again!'
            
            html = self.html.replace('<!--table-->', context, 1).replace('<!--dataset-->', dataset, 1).replace('<!--sheet-->', sheet_num, 1)
        
        return web.Response(text = html, content_type='text/html')

    async def dataload(self, request: object) -> None:
        """This method makes items at main window and launch update javascript."""
        dataset = request.rel_url.query['dataset']
        sheet_num = request.rel_url.query['sheet']
        id_col = request.rel_url.query['idcolumn']

        if sheet_num.isdigit():
            xl = pd.ExcelFile(dataset)
            df = xl.parse(xl.sheet_names[int(sheet_num)])
            if id_col.isdigit():
                df = df[df.columns[int(id_col)]]
                context = ''
                for val in df:
                    context += f'<div class="items" id="i{val}">{val}</div>'
            self.items = context
            context += '<script>window.onload = update</script>'
            asyncio.create_task(main(dataset, 0, int(sheet_num), int(id_col), self))

            html = self.html.replace('<!--items-->', context, 1).replace('<!--dataset-->', dataset, 1).replace('<!--sheet-->', sheet_num, 1)
    
        return web.Response(text = html, content_type='text/html')
    
    async def update(self, request: object) -> None:
        """This method sends jsons to browser as soon as data loaded then flushing list."""

        result = json.dumps(self.mainstream)
        self.mainstream.clear()
        return web.Response(text = result, content_type='application/json')

class consoler:
    """One more subscriber for print progress to console"""
    async def event(self, val):
        print(val)
    

@app.command()
def upload(from_file: Annotated[str, typer.Option('--file', '-f')],
           from_column: Annotated[int, typer.Option('--column', '-c')], 
           idx: Annotated[int, typer.Option('--idx', '-i')] = 0) -> None:
    """Upload data to esup"""

    xl = pd.ExcelFile(from_file)
    df = xl.parse(xl.sheet_names[2])
    df2 = df[[df.columns[idx], df.columns[from_column]]]
    

@app.command()
def download(to_file: Annotated[str, typer.Option('--file', '-f')], 
             to_column: Annotated[int, typer.Option('--column', '-c')]=None, 
             to_sheet: Annotated[int, typer.Option('--sheet', '-s')]=0,
             idx: Annotated[int, typer.Option('--idx', '-i')] = 0) -> None:
    """Donloads data from esup"""

    from_time = time.time()
    cnt = asyncio.run(main(to_file, to_column, to_sheet, idx))
    print(f'Completed in {time.time() - from_time} seconds for {cnt} items')
    
async def main(to_file: str, to_column: int, to_sheet: int, idx: int, subscriber=None) -> int:
    """Function launch all coroutins for downloads and uploads values"""

    xl = pd.ExcelFile(to_file)
    df = xl.parse(xl.sheet_names[to_sheet])
    df = df[df.columns[idx]]
    
    try:
        activites = pickle.load(open('cache.pickle', 'rb'))
    except(OSError, IOError) as e:
        activites = dict()

    tasks = []
    line = {
        'FindByActivityId': '',
        'GridRead': 'Готовность к работам по БС',
        'EsupTask': '',
        'getValue': 'Дата готовности к работам по БС'
       }
    
    cnt = 0
    for val in df:
        if not activites.get(val, False):
            activites[val] = extruder(val)

        if not subscriber is None:
            await activites[val].addsubscriber(subscriber)
        else:
            await activites[val].addsubscriber(consoler())
        
        cnt += 1
        tasks.append(asyncio.create_task(activites[val].pipeline(line)))

    results = await asyncio.gather(*tasks)
    finded_datas = dict()
    for res in results:
        if res['func']=='getValue' and res['status']:
            finded_datas[res['value'][0]] = edate(res['value'][1]).datetime()

    df = pd.DataFrame.from_dict({'esupid':list(finded_datas.keys()), 'dates':list(finded_datas.values())})
    df.to_csv(os.path.dirname(to_file) + '/datas.csv')

    for v in activites.values():
        v.clear()
    
    with open('cache.pickle', 'wb') as f:
        pickle.dump(activites, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    if not subscriber is None:
        await subscriber.event({'func': 'main', 'status': True, 'id': 'fin', 'value': cnt})
    return cnt


@app.callback(invoke_without_command=True)
def web_server(ctx: typer.Context) -> None:
    """If option did't present we will start web server"""

    if ctx.invoked_subcommand is None:
        web_content = webmethods(DATA)
        wapp = web.Application()
        wapp.add_routes([web.get('/', web_content.index),
                         web.get('/file', web_content.fileupload),
                         web.get('/tableopen', web_content.tableopen),
                         web.get('/dataload', web_content.dataload), 
                         web.get('/update', web_content.update)])
        os.system('start http://localhost:9999')
        web.run_app(wapp, port=9999)

"""Web interface"""
DATA = """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
xNGS7O+CL8f0C4IZ0="""

if __name__ == '__main__':
    app()


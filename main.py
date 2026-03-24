import datetime
import json
import os
import sys
import gzip
import shutil
import multiprocessing as mp
import requests
import pandas as pd
import socket
import struct
import cv2
from tqdm import tqdm


USER_TOKEN = open('user_token.txt').read().strip()
# 频道分类，从epgpage/6300005884.json获取
URL_CHANNEL_GROUPS = {
    '超高清频道': '/epgcategory/6000009474.json',
    # '全部频道': '/epgcategory/6000000178.json',
    # '回看频道': '/epgcategory/6000000328.json',
    'CCTV频道': '/epgcategory/6000004362.json',
    'BRTV频道': '/epgcategory/6000004363.json',
    '卫视频道': '/epgcategory/6000004364.json',
    '体验频道': '/epgcategory/6000004365.json'
}
URL_CHANNEL_TIMESHIFT = '/epgcategory/6000000328.json'

AUTH_SERVER = 'http://210.13.0.147:8080'
EPG_SERVER = 'http://210.13.21.3'
USER_AGENT = 'okhttp/3.3.1'


def update_channel_list(test=False):
    req = requests.post(
        AUTH_SERVER + '/bj_stb/V1/STB/channelAcquire',
        data=json.dumps({'UserToken': USER_TOKEN}),
        headers={'User-Agent': USER_AGENT}
    )
    res = req.json()
    count_all = len(res["channleInfoStruct"])
    if res['returnCode'] != 0:
        print('无法获取信息')
        return
    # 就是channleInfoStruct，原接口如此，虽然拼错了
    elif len(res['channleInfoStruct']) == 0:
        print('没有频道')
        return

    df = pd.DataFrame(res['channleInfoStruct']).set_index('channelID')
    df['group'] = '无分类'
    df['timeShiftPublished'] = False
    for group_name, url in URL_CHANNEL_GROUPS.items():
        req = requests.get(
            EPG_SERVER + url,
            headers={'User-Agent': USER_AGENT}
        )
        res = req.json()
        for each in res['epgCategorydtl']:
            df.loc[each['code'], 'group'] = group_name
    req = requests.get(
        EPG_SERVER + URL_CHANNEL_TIMESHIFT,
        headers={'User-Agent': USER_AGENT}
    )
    res = req.json()
    for each in res['epgCategorydtl']:
        df.loc[each['code'], 'timeShiftPublished'] = True
    print(f'已获取频道: {count_all}')
    df['rtpAvailable'] = None
    df['rtspAvailable'] = None
    if test:
        count_rtp_available = 0
        count_rtsp_available = 0
        for row in tqdm(df.itertuples(), total=len(df), desc='测试频道可用性'):
            if test_rtp(row.channelURL):
                count_rtp_available += 1
                df.loc[row.Index, 'rtpAvailable'] = True
            else:
                df.loc[row.Index, 'rtpAvailable'] = False
            if test_rtsp(row.timeShiftURL):
                count_rtsp_available += 1
                df.loc[row.Index, 'rtspAvailable'] = True
            else:
                df.loc[row.Index, 'rtspAvailable'] = False
        print(f'直播可用频道数: {count_rtp_available}')
        print(f'回看可用频道数: {count_rtsp_available}')
    df.to_csv(os.path.join('data', 'channels.csv'))

def test_rtp(url, timeout=3):
    ip, port = url.replace('igmp://', '').split(':')
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', int(port)))
    mreq = struct.pack("4sl", socket.inet_aton(ip), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.settimeout(timeout)
    try:
        sock.recvfrom(8192)
        return True
    except socket.timeout:
        return False
    finally:
        sock.close()
        
def test_rtsp_worker(url, queue):
    os.environ['OPENCV_FFMPEG_LOGLEVEL'] = '-8'
    with open(os.devnull, 'w') as devnull:
        # 重定向标准错误/输出到 null
        old_stderr = sys.stderr
        old_stdout = sys.stdout
        sys.stderr = devnull
        sys.stdout = devnull
        try:
            cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
            if cap.isOpened():
                ret, _ = cap.read()
                cap.release()
                queue.put(ret)
            else:
                queue.put(False)
        except Exception:
            queue.put(False)
        finally:
            sys.stderr = old_stderr
            sys.stdout = old_stdout

def test_rtsp(url, timeout=10):
    queue = mp.Queue()
    p = mp.Process(target=test_rtsp_worker, args=(url, queue))
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.terminate()  # 强制终止
        p.join()
        return False
    try:
        return queue.get_nowait()
    except:
        return False

def save_m3u8():
    df = pd.read_csv(os.path.join('data', 'channels.csv'))
    output=os.path.join('results', 'iptv.m3u8')
    with open(output, 'w', encoding='utf-8') as f:
        f.write('#EXTM3U x-tvg-url="https://raw.githubusercontent.com/BoyInTheSun/BeijingUnicomIPTV/refs/heads/main/results/epg.xml.gz"\n')
        for index, row in df.sort_values('userChannelID').iterrows():
            text = '#EXTINF:-1 '
            text += f'tvg-id="{row["userChannelID"]}" '
            text += f'tvg-chno="{row["userChannelID"]}" '
            text += f'tvg-name="{row["channelName"]}" '
            if ((row['rtspAvailable'] is None) and row['timeShift']) or row['rtspAvailable']:
                text += f'catchup-type="default" catchup-days="7" catchup-source="{row["timeShiftURL"]}?playseek={{utc:YmdHMS}}-{{utcend:YmdHMS}}" '
            else:
                text += 'catchup-type="disabled" '
            text += f'group-title="{row["group"]}" '
            text += f'tvg-logo="",'
            text += f'{row["channelName"]}\n'
            if row['rtpAvailable'] is not None and not row['rtpAvailable']:
                text += '# '
            text += f'{row.channelURL.replace("igmp://", "rtp://")}\n'
            f.write(text)
                
def download_schedule(channel_id, date=datetime.date.today()):
    r = requests.get(
        EPG_SERVER + '/schedules/{}_{}.json'.format(channel_id, date.strftime("%Y%m%d")),
        headers={'User-Agent': USER_AGENT},
    )
    if r.status_code != 200:
        return False
    if not os.path.exists(os.path.join('schedules', channel_id)):
        os.mkdir(os.path.join('schedules', channel_id))
    with open(os.path.join('schedules', channel_id, date.strftime("%Y%m%d") + '.json'), 'w', encoding='utf-8') as f:
        f.write(r.text)
    return True

def date_after(start_date, end_date):
    dt = start_date
    print(end_date)
    while dt >= end_date:
        yield dt
        dt -= datetime.timedelta(days=1)
        
def dates_generator(start_date, after_days, before_days):
    assert after_days >= 0 and before_days >= -1
    # 含当天，offset为正则向后，负则向前，0则无限向前
    if before_days >= 0:
        for i in range(after_days, -before_days - 1, -1):
            yield start_date + datetime.timedelta(days=i)
    else:
        # 前一天至无限
        dt = start_date + datetime.timedelta(days=after_days)
        while True:
            yield dt
            dt -= datetime.timedelta(days=1)

def download_all_schedules(start_date=datetime.date.today(), after_days=7, before_days=7):
    channel_ids = pd.read_csv(os.path.join('data', 'channels.csv'))['channelID'].tolist()
    for channel_id in tqdm(channel_ids, desc='下载节目单'):
        if before_days >= 0:
            for date in tqdm(list(dates_generator(start_date, after_days, before_days)), leave=False, desc=f'频道 {channel_id}', unit='天'):
                download_schedule(channel_id, date)
        else:
            for date in tqdm(dates_generator(start_date, after_days, before_days), leave=False, desc=f'频道 {channel_id}', unit='天'):
                is_has_date = download_schedule(channel_id, date)
                if not is_has_date and date < start_date:
                    break

def save_epg():
    df = pd.read_csv(os.path.join('data', 'channels.csv'), index_col='channelID')
    df.sort_values('userChannelID', inplace=True)
    channel_ids = df.index.tolist()
    with open(os.path.join('results', 'epg.xml'), 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>')
        f.write('<tv generator-info-name="sunboy" generator-info-url="sunboy">')
        for channel_id in tqdm(channel_ids, desc='保存EPG'):
            user_channel_id = df.loc[channel_id, 'userChannelID']
            user_channel_id = df.loc[channel_id, 'userChannelID']
            channel_name = df.loc[channel_id, 'channelName']
            f.write('<channel id="{}"><display-name lang="zh">{}</display-name></channel>'.format(
                user_channel_id,
                channel_name
            ))
            for date in list(dates_generator(datetime.date.today(), after_days=7, before_days=7)):
                schedule_file = os.path.join('schedules', str(channel_id), date.strftime("%Y%m%d") + '.json')
                if not os.path.exists(schedule_file):
                    continue
                with open(schedule_file, 'r', encoding='utf-8') as f1:
                    schedules = json.load(f1).get('schedules')
                    for schedule in schedules:
                        f.write('<programme start="{} +0800" stop="{} +0800" channel="{}">'.format(
                            datetime.datetime.strptime(schedule['starttime'],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'),
                            datetime.datetime.strptime(schedule['endtime'],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'),
                            user_channel_id
                        ))
                        f.write('<title lang="zh">{}</title>'.format(schedule.get('title', '暂无信息').replace('<', '《').replace('>', '》').replace('&', '-')))
                        f.write('</programme>')
        f.write('</tv>')
    with gzip.open(os.path.join('results', 'epg.xml.gz'), 'wb') as f_out:
        with open(os.path.join('results', 'epg.xml'), 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)
if __name__ == '__main__':
    for dir_names in ['data', 'schedules', 'results']:
        if not os.path.isdir(dir_names):
            os.mkdir(dir_names)
    # update_channel_list(test=True)
    save_m3u8()
    # download_all_schedules(before_days=-1)
    # download_all_schedules()
    save_epg()
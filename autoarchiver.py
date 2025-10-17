import sqlite3
import sys

import livestream_poller as lsp
import threading
import subprocess
import os
import time
import json

def twitch_thread(channel_id, vods_dir, formatstr):
    print("I am going to download a twitch stream from ", channel_id)
    ret = subprocess.run(["yt-dlp", "--embed-metadata", "-o", vods_dir+os.sep+twitchformatstr, lsp.createTwitchPollRoute(channel_id)] + (["--cookies", cookies_file] if cookies_file is not None else []))

def ytarchive_thread(args, database, vods_dir, formatstr):
    print("I am going to download ", f"https://youtube.com/watch?={args[0]}")
    ret = subprocess.run(["ytarchive", "-w", "--add-metadata", "-o", vods_dir+os.sep+formatstr, f"https://youtube.com/watch?v={args[0]}","best"] + (["--cookies", cookies_file] if cookies_file is not None else []))

    if ret.returncode == 0:
        with sqlite3.connect(database) as con:
            con.execute(f"INSERT INTO vods {args}")
            con.commit()

def spawn_twitch_thread(channel_id, vods_dir, formatstr):
    t = threading.Thread(target=twitch_thread, args=(channel_id, vods_dir, formatstr))
    t.start()   

def spawn_ytarchive_thread(video_id, channel_id, database, vods_dir, formatstr):
    t = threading.Thread(target=ytarchive_thread, args=((video_id, channel_id), database, vods_dir, formatstr))
    t.start()

def twitch_worker_thread(channel, polltime, vods_dir, formatstr):
    while True:
        print("Checking twitch channel", channel)
        try:
            res = lsp.pollTwitchStatus(channel)
        except Exception as ex:
            print(ex)
            time.sleep(polltime)
            continue
        
        if(res):
            # actually this doesn't need to be async so instead of spawning a child thread just go at it directly
            print(f"downloading twitch stream: {channel}")
            twitch_thread(channel, vods_dir, formatstr)
            
        time.sleep(polltime)

def worker_thread(channel, database, polltime, vods_dir, formatstr):
    active_downloads = []
    channelid = lsp.getChannelId(channel)
    with sqlite3.connect(database) as con:
        while True:
            print("Checking channel ", channel)
            try:
                err, result = lsp.pollLivestreamStatus(channelid)
            except Exception as ex:
                print(ex)
                time.sleep(polltime)
                continue
            if err is None:
                print("Found video ", result.title, " ", result.live)
                if result.live == lsp.STREAM_STATUS.STARTING_SOON or result.live == lsp.STREAM_STATUS.LIVE:
                    if result.id not in active_downloads:
                        if not con.cursor().execute(f"SELECT EXISTS(SELECT 1 FROM vods WHERE video = '{result.id}')").fetchall()[0][0]: #video not already in database
                            print("Downloading ", result.title, " from ", channel)
                            spawn_ytarchive_thread(result.id, channelid, database, vods_dir, formatstr)
                            active_downloads.append(result.id) # we don't actually need to clear this when done
            else:
                print("Error: ", err)

            time.sleep(polltime)

with open("config.json") as fp:
    config = json.load(fp)
    if "vods_dir" in config:
        vods_dir = config["vods_dir"]
    else:
        vods_dir = "." + os.sep

    if "gid" in config:
        try:
            os.setgid(config["gid"])
        except:
            pass
    if "umask" in config:
        try:
            os.umask(config["umask"])
        except:
            pass
    if "database" in config:
        database = config["database"]
    else:
        database = "vods.db"
    if "polltime" in config:
        polltime = config["polltime"] * 60
    else:
        polltime = 1 * 60
    if "format" in config:
        formatstr = config["format"]
    else:
        formatstr = "%(channel)s"+os.sep+"[%(start_date)s] %(title)s-%(id)s"
    if "twitchformatstr" in config:
        twitchformatstr = config["twitchformatstr"]
    else:
        twitchformatstr = "%(uploader)s" +os.sep + "[%(upload_date>%Y-%m-%d)s] %(description)s - %(id)s.%(ext)s"

    if "cookies" in config:
        # test cookies file
        try:
            f = open(config["cookies"], "r")
            f.close
            cookies_file = config["cookies"]
        except:
            print("Couldn't load cookies file", config["cookies"])
            cookies_file = None
    else:
        cookies_file = None

try:
    print("Testing ytarchive")
    subprocess.run(["ytarchive", "--version"])
except:
    print("YTARCHIVE MISSING. GOODBYE")
    #sys.exit(1)

#if database doesn't already exist, create it
with sqlite3.connect(database) as con:
    foo = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vods'").fetchall()
    print(foo)
    if not foo:
        print("creating database...")
        con.execute("CREATE TABLE vods (video text, channel text)")
        con.commit()


for channel in config["channels"]:
    print("Starting thread for channel", channel)
    threading.Thread(target=worker_thread, args=(channel, database, polltime, vods_dir, formatstr)).start()

if "twitch" in config:
    for channel in config["twitch"]:
        print("Starting thread for twitch channel", channel)
        threading.Thread(target=twitch_worker_thread, args=(channel, polltime, vods_dir, formatstr)).start()

while True:
    # this would be a great spot to mine some bitcoin
    time.sleep(100)

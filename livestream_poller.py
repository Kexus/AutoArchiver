#lovingly stolen from https://github.com/saplinganon/imissfauna.com/blob/master/server/livestream_poller.js
import datetime
import json
import re
import requests
import bs4
from enum import Enum
import time

import subprocess

try:
    subprocess.run(["yt-dlp", "--version"])
    have_ytdlp = True
except:
    have_ytdlp = False

class STREAM_STATUS(Enum):
    OFFLINE = 1
    INDETERMINATE = 2
    STARTING_SOON = 3
    LIVE = 4

class STREAM_TYPE(Enum):
    LIVE_STREAM = 1
    PREMIERE = 2
    DEAD = 3

class Results:
    def __init__(self, error=None, result=None, live=None, title=None, videoLink=None, thumbnail=None, isMembersOnly=None, streamType=None, streamStartTime=None, id=None):
        self.error = error
        self.result = result
        self.live = live
        self.title = title
        self.videoLink = videoLink
        self.thumbnail = thumbnail
        self.isMembersOnly = isMembersOnly
        self.streamType = streamType
        self.streamStartTime = streamStartTime
        self.id = id

    def __repr__(self):
        return f"{self.live} {self.title}"

    def __str__(self):
        return self.__repr__()

def getChannelId(channelID):
    if re.search(r"[a-zA-Z\-_0-9]{24}$", channelID):
        return channelID
    else:
        resp = requests.get(f"https://youtube.com/{'c/' if channelID[0] != '@' else ''}{channelID}/")
        dom = bs4.BeautifulSoup(resp.text, 'html.parser')
        try:
            canonical = dom.select_one("link[rel='canonical']").get("href")
        except Exception:
            print(f"Failed @ {channelID}!!!!!!!!")
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            raise

        return canonical.split("/")[-1]

def createTwitchPollRoute(channelID):
    return f"https://www.twitch.tv/{channelID}"

def pollTwitchStatus(channelID):
    if have_ytdlp:
        return len(subprocess.run(["yt-dlp", createTwitchPollRoute(channelID), "-s"], capture_output=True).stderr) == 0
    else:
        return 'isLiveBroadcast' in requests.get(createTwitchPollRoute(channelID)).text

def createPollRoute(channelID):
    if re.search(r"[a-zA-Z\-_0-9]{24}$", channelID):
        return f"https://www.youtube.com/channel/{channelID}/live"
    else:
        if channelID[0] == "@":
            url = f"https://www.youtube.com/{channelID}/live"
        else:
            url = f"https://www.youtube.com/c/{channelID}/live"
        resp = requests.get(url)
        if resp.status_code != 200:
            return None
        dom = bs4.BeautifulSoup(resp.text, 'html.parser')
        canonical = dom.select_one("link[rel='canonical']").get("href")

        if "watch?v=" in canonical:
            return canonical

        # uh oh, that means /live is still broken and we're actually at /streams
        # look for LIVE badge, if it exists
        # there is no dom yet because the js hasn't been rendered so we need to dig for the json instead

        scripts = dom.findAll("script")

        try:
            for script in scripts:
                if script.string and "ytInitialData" in script.string[0:30]: # we know its at the beginning so truncate
                    # find starting {
                    jsontext = script.string[script.string.find("{"):script.string.rfind("}")+1]
                    jsn = json.loads(jsontext)

                    temp = jsn["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][3]["tabRenderer"]
                    if "contents" in temp:
                        thumbnails = jsn["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][3]["tabRenderer"]["contents"]

                        # upcoming streams may be listed before live streams
                        # first, look for only live streams
                        for thumb in thumbnails:
                            if 'continuationItemRenderer' in thumb:
                                # hit the end
                                break
                            overlays = thumb["richItemRenderer"]["content"]["videoRenderer"]["thumbnailOverlays"]

                            for overlay in overlays:
                                if "thumbnailOverlayTimeStatusRenderer" in overlay:
                                    if overlay["thumbnailOverlayTimeStatusRenderer"]["style"] == "LIVE":
                                        return f'https://www.youtube.com/watch?v={thumb["richItemRenderer"]["content"]["videoRenderer"]["videoId"]}' # found a live one!
                        # didn't find any live streams, look for upcoming streams (hopefully the soonest is listed first)
                        for thumb in thumbnails:
                            if 'continuationItemRenderer' in thumb:
                                # hit the end
                                break
                            overlays = thumb["richItemRenderer"]["content"]["videoRenderer"]["thumbnailOverlays"]

                            for overlay in overlays:
                                if "thumbnailOverlayTimeStatusRenderer" in overlay:
                                    if overlay["thumbnailOverlayTimeStatusRenderer"]["style"] == "UPCOMING":
                                        return f'https://www.youtube.com/watch?v={thumb["richItemRenderer"]["content"]["videoRenderer"]["videoId"]}'

                        # didn't find anything
                        break
                    else: # we got a richGridRenderer instead of a richTabRenderer?
                        thumbnails = jsn["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][3]["tabRenderer"]
                        if 'content' not in thumbnails:
                            break

                        thumbnails = thumbnails["content"]["richGridRenderer"]["contents"]
                        # upcoming streams may be listed before live streams
                        # first, look for only live streams
                        for thumb in thumbnails:
                            if 'continuationItemRenderer' in thumb:
                                # hit the end
                                break
                            overlays = thumb["richItemRenderer"]["content"]["videoRenderer"]["thumbnailOverlays"]

                            for overlay in overlays:
                                if "thumbnailOverlayTimeStatusRenderer" in overlay:
                                    if overlay["thumbnailOverlayTimeStatusRenderer"]["style"] == "LIVE":
                                        return f'https://www.youtube.com/watch?v={thumb["richItemRenderer"]["content"]["videoRenderer"]["videoId"]}' # found a live one!
                        # didn't find any live streams, look for upcoming streams (hopefully the soonest is listed first)
                        for thumb in thumbnails:
                            if 'continuationItemRenderer' in thumb:
                                # hit the end
                                break
                            overlays = thumb["richItemRenderer"]["content"]["videoRenderer"]["thumbnailOverlays"]

                            for overlay in overlays:
                                if "thumbnailOverlayTimeStatusRenderer" in overlay:
                                    if overlay["thumbnailOverlayTimeStatusRenderer"]["style"] == "UPCOMING":
                                        return f'https://www.youtube.com/watch?v={thumb["richItemRenderer"]["content"]["videoRenderer"]["videoId"]}'

                        # didn't find anything
                        break


        except Exception:
            print(f"failed @ {channelID}!!!!!!!!")
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            raise

        return canonical

def extractInitialPlayerResponse(fromScript):
    ss = fromScript.split(";")
    ss = [fromScript]

    for s in ss:
        try:
            return json.loads(s[s.find("{"):-1])
        except:
            print(s[s.find("{"):-1])
            pass

def fetchLivestreamPage(channelID):
    resp = requests.get(createPollRoute(channelID))
    if resp.status_code != 200:
        return f"HTTP status {resp.status_code}", None
    return None, resp.text

def extractLivestreamInfo(fromPageContent):
    dom = bs4.BeautifulSoup(fromPageContent, 'html.parser')
    canonical = dom.select_one("link[rel='canonical']")

    if canonical is None:
        raise Exception("MalformedHTML")

    link = canonical.get("href")
    if "watch?v=" not in link:
        return Results(live=STREAM_STATUS.OFFLINE, streamType=STREAM_TYPE.LIVE_STREAM, isMembersOnly=False)

    liveTitle = dom.select_one("meta[name='title']").get("content")
    response = Results(live=STREAM_STATUS.INDETERMINATE, title=liveTitle, videoLink=link, isMembersOnly=False, streamType=STREAM_TYPE.LIVE_STREAM)
    response.id = link.split("=")[1]

    scripts = dom.select("script")
    playerInfo = None

    for script in scripts:
        if script.text.startswith("var ytInitialPlayerResponse = "):
            playerInfo = extractInitialPlayerResponse(script.text)
            if playerInfo is not None:
                break

    if playerInfo is None:
        print("Couldn't extract ytInitialPlayerResponse")
        return response

    # check if live or just waiting room
    videoDetails = playerInfo["videoDetails"]
    if videoDetails and "isLiveContent" in videoDetails and "isUpcoming" in videoDetails:
        response.live = STREAM_STATUS.STARTING_SOON
    elif videoDetails and "isLiveContent" in videoDetails and "isUpcoming" not in videoDetails:
        response.live = STREAM_STATUS.LIVE

    # check stream frame start time
    # if it's more than one hour out, act as if it was offline
    if "offlineSlate" in playerInfo["playabilityStatus"]["liveStreamability"]["liveStreamabilityRenderer"]:
        ts = playerInfo["playabilityStatus"]["liveStreamability"]["liveStreamabilityRenderer"]["offlineSlate"]["liveStreamOfflineSlateRenderer"]["scheduledStartTime"]
        expectedStartTime = int(ts) * 1000
        waitTimeLeftMS = expectedStartTime - int(time.time() * 1000)
        response.streamStartTime = datetime.datetime.fromtimestamp(expectedStartTime//1000)

        if waitTimeLeftMS > 1800 * 1000:
            response.live = STREAM_STATUS.OFFLINE

    thumbnailArray = None
    try:
        thumbnailArray = videoDetails["thumbail"]["thumbnails"]
    except:
        pass

    if thumbnailArray is not None:
        for t in thumbnailArray:
            try:
                if int(t["width"]) > 300 and int(t["height"]) > 150:
                    response.thumbnail = t["url"]
                    break
            except:
                pass

    return response

def pollLivestreamStatus(channelID):
    error, url = fetchLivestreamPage(channelID)
    if error:
        return error, None
    return None, extractLivestreamInfo(url)

if __name__ == "__main__":
    res, ret = pollLivestreamStatus("ChisakaAiri")


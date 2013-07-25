import json
import sys
import urllib, urllib2

class DontRedirect(urllib2.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if code in (301, 302, 303, 307):
            raise urllib2.HTTPError(req.get_full_url(), code, msg, headers, fp)

class Connection(object):    
    def __init__(self, api_key=None):
        self.host = 'api.nytimes.com'
        self.api_key = api_key
        (major, minor, micro, releaselevel, serial) = sys.version_info
        self.user_agent = "Python/%d.%d.%d" % (major, minor, micro)
    
    def get_mostviewed(self, section):
        params = dict(offsset=0)
        data = self._call(self.host, 'svc/mostpopular/v2/mostviewed/'+ section +'/1.json', params)
        return data

    def search_by_url(self, url):
        fq = 'web_url:("' + url + '")'
        params = dict(fq=fq)
        data = self._call(self.host, 'svc/search/v2/articlesearch.json', params)
        return data

    def get_comments(self, url):
        params = dict(url=url)
        data = self._call(self.host, 'svc/community/v2/comments/url/exact-match.json', params)
        return data

    def _get(self, url, timeout):
        dont_redirect = DontRedirect()
        opener = urllib2.build_opener(dont_redirect)
        #opener.addheaders = [('User-agent', user_agent + ' urllib')]
        try:
            response = opener.open(url)
            code = response.code
            data = response.read()
        except urllib2.URLError, e:
            return 500, str(e)
        except urllib2.HTTPError, e:
            code = e.code
            data = e.read()
        return code, data

    def _call(self, host, method, params={}, timeout=10000):
        scheme = 'http'
        params['api-key'] = self.api_key
        
        request = "%(scheme)s://%(host)s/%(method)s?%(params)s" % {
            'scheme': scheme,
            'host': host,
            'method': method,
            'params': urllib.urlencode(params, doseq=1)
            }

        http_response = self._get(request, timeout)
        data = json.loads(http_response[1])
        return data

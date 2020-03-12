import scrapy
from w3lib.html import remove_tags as rt
import json
import csv
import html


class ConfSpider(scrapy.Spider):
    name = 'get_conf'
    conf_name = 'xxx2019'
    index_url = 'https://www.xxxxxxxxxxxxx.com/api/iplanner/index.php'
    link = f"https://www.xxxxxxxxxxxxx.com/{conf_name}/iplanner/#/grid"

    @staticmethod
    def parse_simple_authors(authors_raw, important, is_moder):
        c = '('
        i = ':'
        k = ','
        res = []

        authors_raw = authors_raw.split('<br>')
        for a in authors_raw:
            if c in a and i in a:
                spl = a.split(i)
                role = spl[0]
                spl = spl[1].split(c)
                name = spl[0].strip()
                affil = spl[1].replace(')', '').strip()
                res.append((name, affil, role))
            elif i in a and k in a and c not in a:
                spl = a.split(i)
                role = spl[0]
                spl = spl[1].split(k)
                name = spl[0]
                affil = spl[1]
                res.append((name, affil, role))
            elif c in a and i not in a:
                spl = a.split(c)
                name = spl[0].strip()
                affil = spl[1].replace(')', '').strip()
                if len(authors_raw) == 1 and not is_moder:
                    role = important
                elif is_moder:
                    role = 'Moderator'
                else:
                    role = 'Abstract author'
                res.append((name, affil, role))
            elif k in a:
                spl = a.split(',', 1)
                name = spl[0]
                affil = spl[1]
                if len(authors_raw) == 1 and not is_moder:
                    role = important
                elif is_moder:
                    role = 'Moderator'
                else:
                    role = 'Abstract author'
                res.append((name, affil, role))
            else:
                if len(authors_raw) == 1 and not is_moder:
                    role = important
                elif is_moder:
                    role = 'Moderator'
                else:
                    role = 'Abstract author'
                res.append((a, '', role))
        return res

    def start_requests(self):
        form = {
            'conf': self.conf_name,
            'method': 'get',
            'model': 'index',
        }
        yield scrapy.FormRequest(self.index_url, formdata=form, callback=self.get_index)

    def get_index(self, response):
        index = response.body_as_unicode()
        index = json.loads(index)

        for key in index['sessions']:
            for session in index['sessions'][key]:
                sid = session['id']

                form = {
                    'conf': self.conf_name,
                    'method': 'get',
                    'model': 'sessions',
                    'params[sids]': sid,
                }
                yield scrapy.FormRequest(self.index_url, formdata=form, callback=self.get_sessions)

    def get_sessions(self, response):
        sessions = response.body_as_unicode()
        sessions = json.loads(sessions)

        for key in sessions:
            cur_session = sessions[key]
            session_name = cur_session['title']
            descr = cur_session['type']

            if 'pers' in cur_session:
                for sid in cur_session['pers']:
                    person_raw = cur_session['pers'][sid]['text']
                    authors = self.parse_simple_authors(person_raw, 'Moderator', True)
                    with open('sessions.csv', 'a') as out:
                        csv_out = csv.writer(out, delimiter='|')

                        for a in authors:
                            row = [
                                a[0],
                                a[1],
                                a[2].replace('Chair', 'Moderator'),
                                session_name,
                                descr,
                                self.link
                            ]
                            csv_out.writerow(row)

            if 'pres' in cur_session:
                for k in cur_session['pres']:
                    pid = cur_session['pres'][k]['id']

                    form = {
                        'conf': self.conf_name,
                        'method': 'get',
                        'model': 'presentation',
                        'params[pid]': str(pid),
                    }
                    yield scrapy.FormRequest(self.index_url, formdata=form, callback=self.get_presentations)

    def get_presentations(self, response):
        present = response.body_as_unicode()
        present = json.loads(present)

        def parse_authors(authors_raw, affil_raw, important):
            res = []
            if '<sup>' in authors_raw:
                affils_d = {}
                affil_raw = affil_raw.split(', <sup>')
                for aff in affil_raw:
                    spl = aff.split('</sup>')
                    af_idx = spl[0].replace('<sup>', '').strip('\n')
                    affils_d[af_idx] = spl[1].strip('\n')

                authors_raw = authors_raw.split('</sup>')
                del authors_raw[-1]

                for auth in authors_raw:
                    auth = auth.strip(',').strip()
                    role = important if '<u>' in auth else 'Abstract author'
                    spl = auth.split('<sup>')
                    name = rt(spl[0])
                    affil = ''
                    aff_idx_l = spl[1].split(',')
                    for idx in aff_idx_l:
                        affil += f"{affils_d[idx]} & "
                    res.append((name, affil.strip(' &'), role))
            else:
                names_l = authors_raw.split(',')
                for name in names_l:
                    role = important if '<u>' in name else 'Abstract author'
                    res.append((rt(name), affil_raw, role))
            return res

        hu = html.unescape

        title = present['title']
        abstract = rt(present['text']) if present['text'] else ''
        session_name = present['stitle']
        descr = hu(present['type']) if present['type'] else ''
        important = 'Poster presenter' if 'poster' in descr.lower() else 'Speaker'

        authors_raw = present['aut']
        if authors_raw:
            affils_raw = present['inst']

            if '</' in authors_raw:
                authors = parse_authors(authors_raw, affils_raw, important)
            else:
                authors = self.parse_simple_authors(authors_raw, important, False)

            with open('presents.csv', 'a') as out:
                csv_out = csv.writer(out, delimiter='|')

                for a in authors:
                    row = [
                        hu(a[0]),
                        hu(a[1]),
                        hu(a[2]),
                        '',
                        hu(session_name),
                        hu(descr),
                        hu(title),
                        hu(abstract),
                        self.link
                    ]
                    csv_out.writerow(row)

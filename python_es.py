# -*- coding:utf-8 -*-

from store_to_elasticsearch import get_es_client
import logging
import sys
from get_coordinate import getGeoPoints, getAddressInfo
from tianditu_coordinate import tiandituPoint
from elasticsearch import helpers
from multiprocessing import Process

reload(sys)
sys.setdefaultencoding('utf-8')

logger = logging.getLogger(__name__)

es = get_es_client()

province_code = {"11": u"北京市", "12": u"天津市", "13": u"河北省", "14": u"山西省", "15": u"内蒙古", "21": u"辽宁省", "22": u"吉林省",
                 "23": u"黑龙江省", "31": u"上海市", "32": u"江苏省", "33": u"浙江省", "34": u"安徽省", "35": u"福建省", "36": u"江西省",
                 "37": u"山东省", "41": u"河南省", "42": u"湖北省", "43": u"湖南省", "44": u"广东省", "45": u"广西壮族", "46": u"海南省",
                 "50": u"重庆市", "51": u"四川省", "52": u"贵州省", "53": u"云南省", "54": u"西藏", "61": u"陕西省", "62": u"甘肃省",
                 "63": u"青海省", "64": u"宁夏回族", "65": u"新疆维吾尔", "66": u"新疆建设兵团", "71": u"台湾省", "81": u"香港特别行政区",
                 "82": u"澳门特别行政区"}


def get_city2():
    """获取城市map{id:name}"""
    dict = {}
    sql = '''{"query":{"bool":{"must":[{"match_all":{}}],"must_not":[],"should":[]}},"from":0,"size":5000,"sort":[],"aggs":{}}
    '''
    try:
        results = es.search("region_metadata_2017_cn", "meta", sql)
        if results['hits']['total'] > 0:
            data_list = results['hits']['hits']
            for data_source in data_list:
                data = data_source['_source']
                city_id = data['city_id']
                city = data['city']
                dict[city_id] = city
    except Exception as e:
        logger.debug(e)
    dict.pop('110000')
    dict.pop('310000')
    dict.pop('120000')
    dict.pop('500000')
    return dict


def parse_es_data(f, i):
    """按城市匹配修正es的数据"""
    city_dict = get_city2()
    print 'cities-', len(city_dict.keys())
    ids = []
    for key in city_dict.keys():
        dics = []
        prefix = key[0:4]
        print u'%s-' % city_dict[key], prefix
        if prefix not in ids and prefix[0] == i:
            sql = '''{"query":{"bool":{"must":[{"prefix":{"electr_supervise_no":"%s"}}],"must_not":[],"should":[]}},"from":0,"size":10000,"sort":[],"aggs":{}}''' % prefix
            results = es.search("land_transaction_1_cn", "transaction", sql)
            if results['hits']['total'] > 0:
                data_list = results['hits']['hits']
                print len(data_list)
                for data in data_list:
                    id = data['_id']
                    electr_supervise_no = data['_source']['electr_supervise_no']
                    province = province_code[prefix[0:2]]
                    city = data['_source']['city']
                    location = data['_source']['location']
                    data_source_url = data['_source']['data_source_url']
                    dic = {
                        "_index": "land_transaction_1_cn",
                        "_type": "transaction",
                        "_id": id,
                        "_source": {
                            "electr_supervise_no": electr_supervise_no,
                            "province": province,
                            "location": location
                        }
                    }
                    if city == city_dict[key]:
                        flag = 1
                        address = city + location
                        bd_lat, bd_lon = getGeoPoints(address)
                        tdt_lat, tdt_lon = tiandituPoint(address)
                        district = getAddressInfo(bd_lat, bd_lon)[1]
                        dic["_source"]["city"] = city
                        dic["district"] = district
                        dic["geopoint"]["bd_lat"], dic["geopoint"]["bd_lon"] = bd_lat, bd_lon
                        dic["geopoint_tdt"]["tdt_lat"], dic["geopoint_tdt"]["tdt_lon"] = tdt_lat, tdt_lon
                        dic["flag"] = flag
                        if (bd_lat == 0 and bd_lon == 0) or len(electr_supervise_no) <= 9:
                            flag = 0
                            dic["_source"]["data_source_url"] = data_source_url
                            dic["flag"] = flag

                    else:
                        flag = 0
                        dic["_source"]["data_source_url"] = data_source_url
                        dic["flag"] = flag

                    dics.append(dic)

            else:
                print u'%s没有数据' % city_dict[key]
                f.write('\"%s没有数据\",\"city_id-%s\"' % (city_dict[key], key))
                continue

        result = helpers.bulk(es, actions=dics)
        print result
    f.close()


if __name__ == '__main__':
    f = open(u'no_data_city.csv', 'a')
    p1 = Process(target=parse_es_data, args=(f, '1'))
    p2 = Process(target=parse_es_data, args=(f, '2'))
    p3 = Process(target=parse_es_data, args=(f, '3'))
    p4 = Process(target=parse_es_data, args=(f, '4'))
    p5 = Process(target=parse_es_data, args=(f, '5'))
    p6 = Process(target=parse_es_data, args=(f, '6'))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    f.close()

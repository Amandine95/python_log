import json
def set_sign():
    f = open(u'sign.csv', 'w+')
    dic = {
        '1101':'bj',
        '1102':'tj'
    }
    string = json.dumps(dic)
    f.write()
    f.close()


def get_sign():
    f = open(u'sign.csv', 'r+')
    string = f.readlines()
    print type(string)
    for i in string:
        print i


if __name__ == '__main__':
    li = ['1','2','3']
    a = str(li)
    print a[0]
    print type(a)

    # set_sign()
    # get_sign()


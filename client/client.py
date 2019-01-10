import socket
import sys
import rlcompleter
import readline


def run(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((host, port))
        print('Connect to %s:%d' % (host, port))
    except:
        print('Unable to connect %s:%d' % (host, port))
        exit(1)

    print('== Welcome to StellarSQL Client! ==')

    client = Client()

    while client.check_live():
        input = raw_input('StellarSQL> ')
        message = client.parse(input)
        if message is not None:
            s.send(message)
        else:
            continue
        data = s.recv(512)
        print(data)

    s.close()


class Client():
    """
    username||database||query||key
    """

    def __init__(self):
        self._user = ""
        self._database = ""
        self._query = ""
        self._key = ""
        self._is_live = True

    def _set_user(self, name):
        self._user = name
        print("user: %s" % self._user)

    def _use_database(self, name):
        self._database = name
        print("database: %s" % self._database)

    def _create_user(self, name, key):
        self._user = name
        self._key = key
        return ('{0}||||||{1}\n').format(self._user, self._key)

    def _create_database(self, db_name):
        self._database = db_name
        return ('{0}||||create database {1};\n').format(self._user, self._database)

    def _send_query(self, query):
        if self._user == "":
            print('Please set or create user!')
            return None
        if self._database == "":
            print('Please use or create database!')
            return None
        self._query = query
        return ('{0}||{1}||{2};\n').format(self._user, self._database, self._query)

    def check_live(self):
        return self._is_live

    def parse(self, input):
        tokens = input.split()
        try:
            # create user
            # create database
            if tokens[0] == 'create':
                if tokens[1] == 'user':
                    return self._create_user(tokens[2], tokens[3])
                elif tokens[1] == 'database':
                    return self._create_database(tokens[2])
                else:
                    return self._send_query(input)

            # set user
            elif tokens[0] == 'set' and tokens[1] == 'user':
                user = tokens[2]
                self._set_user(user)

            # use database
            elif tokens[0] == 'use':
                db = tokens[1]
                return self._use_database(db)

            # quit
            elif tokens[0] == 'q' or tokens[0] == 'exit':
                self._is_live = False

            elif tokens[0] == 'h' or tokens[0] == 'help':
                print('create user <username> <key>')
                print('set <username>')
                print('create database <db_name>')
                print('use <db_name>')
                print('<query> (ex: select a1 from t1)')

            # use database
            else:
                return self._send_query(input)

        except:
            print('Syntax Error! Enter `h` to see commands.')
            return None

        return None


if __name__ == '__main__':
    host = '127.0.0.1'
    port = 23333

    readline.parse_and_bind("tab: complete")

    if len(sys.argv) == 3:
        host = sys.argv[1]
        port = sys.argv[2]
    elif len(sys.argv) == 1:
        pass
    else:
        print('run: client.py [host] [port]')
        exit(1)

    run(host, port)

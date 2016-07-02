from boto.utils import parse_ts
from boto.glacier import connect_to_region
from boto.glacier.layer2 import Layer2
from boto.glacier.exceptions import UploadArchiveError
import sys
import os
import json
import time
import pymysql

access_key_id = ""
secret_key = ""

db_host = ""
db_port = 3306
db_username = ""
db_password = ""
db_name = ""

def terminate(code):
    raw_input("\nPress enter to continue...")
    sys.exit(code)


def format_bytes(bytes):
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return "%3.1f %s" % (bytes, x)
        bytes /= 1024.0
    return "%3.1f %s" % (bytes, 'TB')


def format_time(num):
    times = []
    for x in [(60, 'second'), (60, 'minute'), (1e10, 'hour')]:
        if num % x[0] >= 1:
            times.append('%d %s%s' % (num % x[0], x[1], 's' if num % x[0] != 1 else ''))
        num /= x[0]
    times.reverse()
    return ', '.join(times)


class Database(object):
    update_sql = "UPDATE `tblDocs` SET `archiveid`=%s, `archivevault`=%s, `archivedate`=%s WHERE `name`=%s"
    select_sql = "SELECT `name` from `tblDocs`"

    def __init__(self, host, port, username, password, name):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.name = name
        self.connection = None
        self.connect()

    def connect(self):
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            db=self.name
        )

    def close(self):
        self.connection.close()
        self.connection = None

    def update(self, file, id, vault):
        print file + ": updating database info... ",
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(self.update_sql,
                               (id, vault.arn, int(time.time()), file))
        except pymysql.MySQLError as exc:
            print "error occured: " + str(exc)
            self.connection.rollback()
        else:
            self.connection.commit()
            print "done. "

    def files(self):
        with self.connection.cursor() as cursor:
            cursor.execute(self.select_sql)
            for row in cursor:
                filename = row[0]
                if filename:
                    yield filename


class Config(object):
    def __init__(self, config_path):
        self.config_path = config_path
        self.read()
        self.validate()

    def read(self):
        with open(self.config_path, 'rU') as f:
            vault_info = f.readline().strip().split('|')
            self.vault_name = vault_info[0]
            self.region = vault_info[1]

            self.dirs = f.readline().strip().split('|')
            self.inventory_job = f.readline().strip()
            self.ls_present = f.readline().strip()

            self.ls = {}
            for file in f.readlines():
                name, id, last_modified, size = file.strip().split('|')
                self.ls[name] = {
                    'id': id,
                    'last_modified': int(last_modified),
                    'size': int(size)
                }

    def write(self):
        with open(self.config_path, 'w') as f:
            f.write(self.vault_name + '|' + self.region + "\n")
            f.write('|'.join(self.dirs) + "\n")
            f.write(self.inventory_job + "\n")
            f.write(self.ls_present + "\n")

            for name, data in self.ls.iteritems():
                f.write(name + "|" + data['id'] + '|' + str(
                    data['last_modified']) + '|' + str(data['size']) + "\n")

    def validate(self):
        # Check some of the values in the config file
        if not self.vault_name:
            print "You need to give a vault name and region in the first line of the config file, e.g. `MyVault|us-west-1`."
            terminate(1)

        if not len(self.dirs):
            print r"You need to give the full path to a folder to sync in the second line of the config file, e.g. `C:\backups`. You can list multiple folders, e.g. `C:\backups|D:\backups`"
            terminate(1)

        for dir in self.dirs:
            if not os.path.exists(dir):
                print "Sync directory not found: " + dir
                terminate(1)


def read_config():
    # Make sure the user passed in a config file
    if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
        print "Config file not found. Pass in a file with the vault name and the directory to sync on separate lines."
        terminate(1)

    config_path = sys.argv[1]
    return Config(config_path)


def main():
    config = read_config()
    # Cool! Let's set up everything.
    connect_to_region(config.region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_key)
    glacier = Layer2(aws_access_key_id=access_key_id, aws_secret_access_key=secret_key, region_name=config.region)
    vault = glacier.get_vault(config.vault_name)
    # workaround for UnicodeDecodeError
    # https://github.com/boto/boto/issues/3318
    vault.name = str(vault.name)
    print "Beginning job on " + vault.arn

    # Ah, we don't have a vault listing yet.
    if not config.ls_present:

        # No job yet? Initiate a job.
        if not config.inventory_job:
            config.inventory_job = vault.retrieve_inventory()
            config.write()
            print "Requested an inventory. This usually takes about four hours."
            terminate(0)

        # We have a job, but it's not finished.
        job = vault.get_job(config.inventory_job)
        if not job.completed:
            print "Waiting for an inventory. This usually takes about four hours."
            terminate(0)

        # Finished!
        try:
            data = json.loads(job.get_output().read())
        except ValueError:
            print "Something went wrong interpreting the data Amazon sent!"
            terminate(1)

        config.ls = {}
        for archive in data['ArchiveList']:
            config.ls[archive['ArchiveDescription']] = {
                'id': archive['ArchiveId'],
                'last_modified': int(float(time.mktime(parse_ts(archive['CreationDate']).timetuple()))),
                'size': int(archive['Size']),
                'hash': archive['SHA256TreeHash']
            }

        config.ls_present = '-'
        config.inventory_job = ''
        config.write()
        print "Imported a new inventory from Amazon."

    database = Database(
        host=db_host,
        port=db_port,
        username=db_username,
        password=db_password,
        name=db_name
    )
    print "Connected to database."
    # Let's upload!
    os.stat_float_times(False)
    try:
        i = 0
        transferred = 0
        time_begin = time.time()
        for dir in config.dirs:
            print "Syncing " + dir
            for file in database.files():
                path = dir + os.sep + file

                if not os.path.exists(path):
                    #print >> sys.stderr, "'%s' does not exist" % path
		    print "\n" + "'%s' does not exist" % path
                    continue

                # If it's a directory, then ignore it
                if not os.path.isfile(path):
                    continue

                last_modified = int(os.path.getmtime(path))
                size = os.path.getsize(path)
                updating = False
                if file in config.ls:

                    # Has it not been modified since?
                    if config.ls[file]['last_modified'] >= last_modified and config.ls[file]['size'] == size:
                        continue

                    # It's been changed... we should delete the old one
                    else:
                        vault.delete_archive(config.ls[file]['id'])
                        del config.ls[file]
                        updating = True
                        config.write()

                try:
                    print file + ": uploading... ",
                    id = vault.concurrent_create_archive_from_file(path, file)
                    config.ls[file] = {
                        'id': id,
                        'size': size,
                        'last_modified': last_modified
                    }

                    config.write()
                    i += 1
                    transferred += size
                    if updating:
                        print "updated."
                    else:
                        print "done."

                    database.update(file, id, vault)

                except UploadArchiveError:
                    print "FAILED TO UPLOAD."

    finally:
        database.close()
        elapsed = time.time() - time_begin
        print "\n" + str(i) + " files successfully uploaded."
        print "Transferred " + format_bytes(transferred) + " in " + format_time(elapsed) + " at rate of " + format_bytes(transferred / elapsed) + "/s."
        terminate(0)

if __name__ == '__main__':
    main()

sync-glacier.py
===============
This is a fork of https://github.com/bitsofpancake/sync-glacier.py which didn't work with PDF files

In addition, it will update a Mysql database with Glacier archive details (see below for schema)

A Python script to easily sync a directory with a vault on Amazon Glacier. This makes it easy to upload a directory of backups, for example, into a vault. This script requires [`boto`](https://github.com/boto/boto) (see their instructions on how to install it) and PyMSQL

pip install pymysql

To use `sync-glacier.py`, first edit `sync-glacier.py` and put in your [Amazon Web Services credentials](https://portal.aws.amazon.com/gp/aws/securityCredentials):
```
access_key_id = ""
secret_key = ""
```

Then, create a configuration file (see `sample.job`) with the vault name, region, and directories you want to sync, separated with `|`.

Run the script and pass in the config file with the command:
```
sync-glacier.py job_file.job
```

On the first run, it will download an inventory of the vault. This takes about four hours, after which you'll need to run the script again. The script will upload the files in the given directory that don't already appear in the vault (or that have been updated since your last upload). Once that's done, every time you want to sync changes to your vault, simply run the script again. It'll detect what's been updated and only upload those files.

NOTE: This script doesn't work very well is you have your files stored in an S3 bucker mounted as a directory with s3fs.  This is because s3fs is not very good at metadata operations, like listing files and directories. The script currently loops through each file in the directory.  As a workaround in this case, you can use sync-glacier2.py, which relies on the database to get metadata instead of the filesystem.

```

CREATE TABLE `tblDocs` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `archiveid` varchar(200) DEFAULT NULL,
  `archivevault` varchar(100) DEFAULT NULL,
  `archivedate` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
) ENGINE=InnoDB AUTO_INCREMENT=13437 DEFAULT CHARSET=latin1;
```
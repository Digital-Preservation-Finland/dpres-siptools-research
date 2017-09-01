# encoding=utf8
"""Writes process reports from mongo DB into json2xml and copies the
reports to the users/reportees home folder"""

import os
import sys
import argparse

from datetime import datetime

from siptools_research import json2xml, database


def parse_arguments(arguments):
    """ Create arguments parser and return parsed command line argumets"""
    parser = argparse.ArgumentParser(
        description="Tool for creating reports")
    parser.add_argument('--output_file', dest='output_file', type=str,
                        default='SIP_report', help='Report output file name')
    parser.add_argument('--home_path', dest='home_path', type=str,
                        default='/home', help='Location of user`s home paths')
    parser.add_argument('--report_dir', dest='report_dir', type=str,
                        default='reports', help='Direcotry of user`s reports')
    parser.add_argument('--from_date', dest='from_date', type=str,
                        default='00010101000000',
                        help='Generate report from date (yyyymmddhhmiss)')
    parser.add_argument('--to_date', dest='to_date', type=str,
                        default=datetime.utcnow().strftime('%Y%m%d%H%M%S'),
                        help='Generate report to date (yyyymmddhhmiss)')
    parser.add_argument('--user_name', dest='user_name', type=str,
                        help=('Generate reports from processes of a certain '
                              'username'))
    parser.add_argument('--report_to_user', dest='to_user', type=str,
                        help=('Write reports to a certain user´s report '
                              'directory'))
    parser.add_argument('--sip_status', dest='sip_status', type=str,
                        help='Generate reports from SIP´s in certain status')

    return parser.parse_args(arguments)


def db_wf_collection():
    """Defines the database and the collection"""
    mongo_db = database.connection(host='localhost')['siptools-research']
    collection = mongo_db['workflow']
    return collection


def get_collection(query={}):
    """Returns the query from the database"""
    return db_wf_collection().find(query)


def build_query(from_date, to_date, username=None, sip_status=None):
    """Builds the query from the arguments given"""
    date_from = datetime.strptime(from_date, '%Y%m%d%H%M%S').isoformat()
    date_to = datetime.strptime(to_date, '%Y%m%d%H%M%S').isoformat()
    query = {"timestamp": {"$gt": date_from, "$lte": date_to}}
    if username:
        query['username'] = username
    if sip_status:
        query['status'] = sip_status
    return query


def print_collections(report_dir, output_file, query):
    """Writes the result of the database query to json2xml"""
    cols = get_collection(query)
    xml_path = os.path.join(report_dir, output_file)
    print "query:%s" % query
    print "report_dir:%s" % xml_path
    if cols.count() > 0:
        json2xml.print_col_to_file(cols, xml_path, 'sip')


def main(arguments=None):
    """The main argument for argparser"""
    args = parse_arguments(arguments)
    output_file = "%s_%s_%s" % (args.output_file, args.from_date, args.to_date)
    if args.user_name is not None:
        print 1
        report_dir = os.path.join(args.home_path, args.to_user or
                                  args.user_name, args.report_dir)
        query = build_query(args.from_date, args.to_date,
                            args.user_name, args.sip_status)
        print_collections(report_dir, output_file, query)
    if args.user_name is None and args.to_user is not None:
        print 2
        report_dir = os.path.join(
            args.home_path, args.to_user, args.report_dir)
        query = build_query(args.from_date, args.to_date,
                            sip_status=args.sip_status)
        print_collections(report_dir, output_file, query)
    if args.user_name is None and args.to_user is None:
        print 3
        for username in os.listdir(args.home_path):
            print "username:%s" % username
            report_dir = os.path.join(
                args.home_path, username, args.report_dir)
            query = build_query(args.from_date, args.to_date,
                                username=username, sip_status=args.sip_status)
            print_collections(report_dir, output_file, query)

    return 0


if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)

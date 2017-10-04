# encoding=utf8
"""Module fow writing output in json xml format"""


def json2xml(json_obj, line_padding=""):
    """Adds data to the output"""
    result_list = list()
    json_obj_type = type(json_obj)

    if json_obj_type is list:
        for sub_elem in json_obj:
            result_list.append(json2xml(sub_elem, line_padding))
        return "\n".join(result_list)

    if json_obj_type is dict:
        for tag_name in json_obj:
            sub_obj = json_obj[tag_name]
            result_list.append("%s<%s>" % (line_padding, tag_name))
            result_list.append(json2xml(sub_obj, "\t" + line_padding))
            result_list.append("%s</%s>" % (line_padding, tag_name))
        return "\n".join(result_list)

    return "%s%s" % (line_padding, json_obj)


def print_tag(tag, line_padding, end_tag=''):
    """Adds XML tags to output"""
    return json2xml('<' + end_tag + tag + '>\n', line_padding)


def print_header():
    """Adds the XML header to the document"""
    return json2xml('<?xml version="1.0" encoding="UTF-8" ?>')


def print_col_to_file(cols, xml_path, doc_name):
    """Writes the data to an output file"""
    with open(xml_path, 'w') as outfile:
        outfile.write(print_header())
        for document in cols:
            outfile.write(print_tag(doc_name, line_padding="\n"))
            outfile.write(json2xml(document, line_padding="\t"))
            outfile.write(print_tag(doc_name, line_padding="\n", end_tag="/"))

# coding=utf-8
"""Imports the SÄHKE2 document and creates an EAD3 finding aid
file from the metadata. All SÄHKE2 elements are imported into
the EAD3 finding aid and the original SÄHKE2 metadata structure
is preserved using the @encodinganalog attribute in the EAD3 finding
aid to annotate the XPath of the element from the original SÄHKE2
document.
"""

import os
import sys
import argparse
import datetime

import lxml.etree as ET

import ipt.version

from siptools.utils import encode_path
import siptools.xml.ead3 as ead3


S2_NS = 'http://www.arkisto.fi/skeemat/Sahke2/2011/12/20'


def s2_ns(tag, child="", grandchild=""):
    """Adds SÄHKE2 namespace to tags"""
    path = '{%s}%s' % (S2_NS, tag)
    if child:
        path = path + '/{%s}%s' % (S2_NS, child)

    if grandchild:
        path = path + '/{%s}%s' % (S2_NS, grandchild)

    return path


def parse_arguments(arguments):
    """Create arguments parser and return parsed
    command line arguments.
    """
    parser = argparse.ArgumentParser(description='A short description of this '
                                     'program')
    parser.add_argument('ead3_location', type=str,
                        help='Location of descriptive metadata')
    parser.add_argument('--ead3_target', dest='ead3_target', type=str,
                        default='./', help=('Digital object (or file) which '
                                            'is the target of descriptive '
                                            'metadata. Default is the root '
                                            'of digital objects'))
    parser.add_argument('--workspace', dest='workspace', type=str,
                        default='./workspace', help='Workspace directory')
    parser.add_argument('--stdout', help='Print output to stdout')

    return parser.parse_args(arguments)


def main(arguments=None):
    """The main method for argparser"""
    args = parse_arguments(arguments)

    target_file = str(args.ead3_location).split('/')[-1]
    new_target = target_file.split('.')[0]
    eventdatetime = datetime.datetime.utcnow().isoformat()

    if args.ead3_target:
        url_t_path = encode_path(args.ead3_target, suffix='-ead3.xml')
    else:
        url_t_path = encode_path(new_target, suffix='-ead3.xml')

    import_xml = ET.parse(args.ead3_location)
    root = import_xml.getroot()

    man_agent = 'import_sahke2.py-%s' % (ipt.version.__version__)
    recordid = 'ead-%s' \
        % root.xpath("/s2:Metadata/s2:TransferInformation/s2:NativeId",
                     namespaces={'s2': S2_NS})[0].text
    titleproper = 'EAD %s' \
        % root.xpath("/s2:Metadata/s2:TransferInformation/s2:Title",
                     namespaces={'s2': S2_NS})[0].text

    ead3_manevent = ead3.ead_maintenanceevent(eventtype='derived',
                                              eventdatetime=eventdatetime,
                                              agenttype='machine',
                                              agent=man_agent)

    ead3_control = ead3.ead_control(recordid=recordid,
                                    title=titleproper,
                                    manstatus='derived',
                                    managencys=['Kansallisarkisto',
                                                'Riksarkivet'],
                                    manevents=[ead3_manevent])

    ead_file = ead3.ead_ead(control=ead3_control,
                            archdesc=create_archdesc(root),
                            relatedencoding=u'SÄHKE2')

    if args.stdout:
        print ead3.serialize(ead_file)

    output_file = os.path.join(args.workspace, url_t_path)
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))

    with open(output_file, 'w+') as outfile:
        outfile.write(ead3.serialize(ead_file))

    print "import_sahke2 created file: %s" % output_file

    return 0


def create_archdesc(parent):
    """Creates the mandatory archdesc element."""
    desc_elements = []

    metadataschema = ead3.ead_element(tag='p',
                                      contents=parent.find(
                                          s2_ns('TransferInformation',
                                                child='MetadataSchema')).text)
    processinfo = ead3.ead_wrapper(
        tag='processinfo',
        contents=[metadataschema],
        encodinganalog='/Metadata/TransferInformation/MetadataSchema')
    desc_elements.append(processinfo)

    dsc = create_dsc(parent)

    ead3_archdesc = ead3.ead_archdesc(level='otherlevel',
                                      did=create_archdescdid(
                                          parent, path='/Metadata'),
                                      desc_elements=desc_elements,
                                      otherlevel='Metadata', dsc=dsc)

    return ead3_archdesc


def create_archdescdid(parent, path):
    """Creates the mandatory top level did element."""
    desc_elements = []

    title = ead3.ead_element(tag='unittitle',
                             contents=parent.find(s2_ns('TransferInformation',
                                                        child='Title')).text,
                             encodinganalog=path +
                             '/TransferInformation/Title')
    desc_elements.append(title)

    natid = ead3.ead_element(tag='unitid',
                             contents=parent.find(
                                 s2_ns('TransferInformation',
                                       child='NativeId')).text,
                             encodinganalog=path +
                             '/TransferInformation/NativeId')
    desc_elements.append(natid)

    contrid = ead3.ead_element(tag='unitid',
                               contents=parent.find(
                                   s2_ns('TransferInformation',
                                         child='TransferContractId')).text,
                               encodinganalog=path +
                               '/TransferInformation/TransferContractId')
    desc_elements.append(contrid)

    repo = s2_contactinformation(parent.find(s2_ns('ContactInformation')),
                                 path=path + '/ContactInformation')
    desc_elements.append(repo)

    cont_part = ead3.ead_element(tag='part',
                                 contents=parent.find(
                                     s2_ns('ContactInformation',
                                           child='Organisation',
                                           grandchild='Name')).text)
    cont_name = ead3.ead_wrapper(tag='name', contents=[cont_part],
                                 relator='Arkistonmuodostaja')
    cont_orig = ead3.ead_wrapper(tag='origination', contents=[cont_name])
    desc_elements.append(cont_orig)

    ead3_did = ead3.ead_did(desc_elements=desc_elements)

    return ead3_did


def create_cdid(parent, path):
    """Creates the mandatory did element for all c elements,
    regardless of the c elements level in the EAD hierarchy.
    """
    langs = []
    desc_elements = []
    unitdates = ['Created', 'Issued', 'Sent', 'Modified', 'Available',
                 'Acquired', 'Finished', 'Accepted', 'Gathered']

    titles = ['Title', 'AlternativeTitle']

    ids = ['NativeId', 'OtherId']

    for ident in parent.findall('*'):
        tag = ET.QName(ident.tag).localname
        if tag in ids:
            ident = ead3.ead_element(tag='unitid',
                                     contents=ident.text,
                                     encodinganalog=path + '/' + tag)
            desc_elements.append(ident)

    for title in parent.findall('*'):
        tag = ET.QName(title.tag).localname
        if tag in titles:
            title = ead3.ead_element(tag='unittitle',
                                     contents=title.text,
                                     encodinganalog=path + '/' + tag)
            desc_elements.append(title)

    for date in parent.findall('*'):
        tag = ET.QName(date.tag).localname
        if tag in unitdates:
            date = ead3.ead_element(tag='unitdate',
                                    contents=date.text,
                                    encodinganalog=path + '/' + tag)
            desc_elements.append(date)

    if parent.find(s2_ns('Valid')) is not None:
        startdate = ead3.ead_element(tag='unitdate',
                                     contents=parent.find(
                                         s2_ns('Valid', child='start')).text,
                                     encodinganalog=path + '/Valid/start')
        desc_elements.append(startdate)
        if parent.find(s2_ns('Valid', child='end')) is not None:
            enddate = ead3.ead_element(tag='unitdate',
                                       contents=parent.find(
                                           s2_ns('Valid', child='end')).text,
                                       encodinganalog=path + '/Valid/end')
            desc_elements.append(enddate)

    if parent.find(s2_ns('Language')) is not None:
        for language in parent.findall(s2_ns('Language')):
            language = ead3.ead_element(tag='language',
                                        contents=language.text)
            langs.append(language)
        langmat = ead3.ead_wrapper(tag='langmaterial', contents=langs,
                                   encodinganalog=path + '/Language')
        desc_elements.append(langmat)

    if parent.find(s2_ns('File')) is not None:
        dao = create_dao(parent.find(s2_ns('File')), path=path + '/File')
        desc_elements.append(dao)

    if parent.find(s2_ns('Format')) is not None:
        for elem in parent.findall(s2_ns('Format', child='*')):
            tag = ET.QName(elem.tag).localname
            elem = ead3.ead_element(tag='physdesc', contents=elem.text,
                                    encodinganalog=path + '/Format/' + tag)
            desc_elements.append(elem)

    if parent.find(s2_ns('MediumID')) is not None:
        mediumid = ead3.ead_element(tag='container',
                                    contents=parent.find(
                                        s2_ns('MediumID')).text,
                                    encodinganalog=path + '/MediumID')
        desc_elements.append(mediumid)

    ead3_did = ead3.ead_did(desc_elements=desc_elements)

    return ead3_did


def create_dsc(parent):
    """Creates the mandatory dsc element that groups all c elements."""
    csubs = []

    for casefile in parent.findall(s2_ns('CaseFile')):
        casefile = create_c01(casefile)
        csubs.append(casefile)

    ead3_dsc = ead3.ead_wrapper(tag='dsc', contents=csubs, dsctype='combined')

    return ead3_dsc


def create_c01(casefile):
    """Creates the c01 element that corresponds to the SÄHKE2
    CaseFile element.
    """
    path = '/Metadata/CaseFile'
    desc_elemlist = desc_elems(casefile, path=path)
    desc_elements = []
    csubs = []

    for item in desc_elemlist:
        desc_elements.append(item)

    if casefile.find(s2_ns('Restriction')) is not None:
        restriction = s2_restriction(casefile.find(s2_ns('Restriction')),
                                     path=path + '/Restriction')
        desc_elements.append(restriction)

    if casefile.find(s2_ns('RetentionPeriod')) is not None:
        retperiod = ead3.ead_element(tag='p',
                                     contents=casefile.find(
                                         s2_ns('RetentionPeriod')).text)
        retentionperiod = ead3.ead_wrapper(tag='appraisal',
                                           contents=[retperiod],
                                           encodinganalog=path +
                                           '/RetentionPeriod')
        desc_elements.append(retentionperiod)

    if casefile.find(s2_ns('RetentionReason')) is not None:
        retreason = ead3.ead_element(tag='p',
                                     contents=casefile.find(
                                         s2_ns('RetentionReason')).text)
        retentionreason = ead3.ead_wrapper(tag='appraisal',
                                           contents=[retreason],
                                           encodinganalog=path +
                                           '/RetentionReason')
        desc_elements.append(retentionreason)

    if casefile.find(s2_ns('RetentionPeriodEnd')) is not None:
        retperiodend = ead3.ead_element(tag='date',
                                        contents=casefile.find(
                                            s2_ns('RetentionPeriodEnd')).text)
        retperiodend_p = ead3.ead_wrapper(tag='p', contents=[retperiodend])
        retentionperiodend = ead3.ead_wrapper(tag='appraisal',
                                              contents=[retperiodend_p],
                                              encodinganalog=path +
                                              '/RetentionPeriodEnd')
        desc_elements.append(retentionperiodend)

    if casefile.find(s2_ns('ElectronicNotification')) is not None:
        electnotif = s2_electronicnotif(
            casefile.find(s2_ns('ElectronicNotification')), path=path)
        desc_elements.append(electnotif)

    if casefile.find(s2_ns('CaseFileRelation')) is not None:
        for crel in casefile.findall(s2_ns('CaseFileRelation')):
            for rel in crel.findall('*'):
                crelation = s2_relations(rel, path=path + '/CaseFileRelation')
                desc_elements.append(crelation)

    if casefile.find(s2_ns('Action')) is not None:
        for action in casefile.findall(s2_ns('Action')):
            action = create_c02(action)
            csubs.append(action)

    if casefile.find(s2_ns('ClassificationScheme')) is not None:
        classlevel = s2_classlevel(casefile.find(
            s2_ns('ClassificationScheme',
                  child='MainFunction')),
                                   path=path +
                                   '/ClassificationScheme/MainFunction')
        fileplan = ead3.ead_wrapper(tag='fileplan', contents=[classlevel],
                                    encodinganalog=path +
                                    '/ClassificationScheme')
        desc_elements.append(fileplan)

    ead3_c = ead3.ead_c(cnum='c01', did=create_cdid(casefile,
                                                    path=path),
                        desc_elements=desc_elements,
                        csubs=csubs, level='otherlevel', otherlevel='CaseFile',
                        encodinganalog=path)

    return ead3_c


def create_c02(action):
    """Creates the c02 element that corresponds to the SÄHKE2
    Action element.
    """
    path = '/Metadata/CaseFile/Action'
    desc_elemlist = desc_elems(action, path=path)
    desc_elements = []
    csubs = []

    for item in desc_elemlist:
        desc_elements.append(item)

    if action.find(s2_ns('Record')) is not None:
        for record in action.findall(s2_ns('Record')):
            record = create_c03(record)
            csubs.append(record)

    ead3_c = ead3.ead_c(cnum='c02', did=create_cdid(action, path=path),
                        desc_elements=desc_elements, csubs=csubs,
                        level='otherlevel', otherlevel='Action',
                        encodinganalog=path)

    return ead3_c


def create_c03(record):
    """Creates the c03 element that corresponds to the SÄHKE2
    Record element.
    """
    path = '/Metadata/CaseFile/Action/Record'
    desc_elemlist = desc_elems(record, path=path)
    desc_elements = []
    audiences = []
    sources = []
    rightslist = []
    protlist = []
    csubs = []

    for item in desc_elemlist:
        desc_elements.append(item)

    if record.find(s2_ns('Restriction')) is not None:
        restriction = s2_restriction(record.find(s2_ns('Restriction')),
                                     path=path + '/Restriction')
        desc_elements.append(restriction)

    if record.find(s2_ns('RetentionPeriod')) is not None:
        retperiod = ead3.ead_element(tag='p',
                                     contents=record.find(
                                         s2_ns('RetentionPeriod')).text)
        retentionperiod = ead3.ead_wrapper(tag='appraisal',
                                           contents=[retperiod],
                                           encodinganalog=path +
                                           '/RetentionPeriod')
        desc_elements.append(retentionperiod)

    if record.find(s2_ns('RetentionReason')) is not None:
        retreason = ead3.ead_element(tag='p',
                                     contents=record.find(
                                         s2_ns('RetentionReason')).text)
        retentionreason = ead3.ead_wrapper(tag='appraisal',
                                           contents=[retreason],
                                           encodinganalog=path +
                                           '/RetentionReason')
        desc_elements.append(retentionreason)

    if record.find(s2_ns('RetentionPeriodEnd')) is not None:
        retperiodend = ead3.ead_element(tag='date',
                                        contents=record.find(
                                            s2_ns('RetentionPeriodEnd')).text)
        retperiodend_p = ead3.ead_wrapper(tag='p', contents=[retperiodend])
        retentionperiodend = ead3.ead_wrapper(tag='appraisal',
                                              contents=[retperiodend_p],
                                              encodinganalog=path +
                                              '/RetentionPeriodEnd')
        desc_elements.append(retentionperiodend)

    if record.find(s2_ns('RecordRelation')) is not None:
        for recrel in record.findall(s2_ns('RecordRelation')):
            for rel in recrel.findall('*'):
                crelation = s2_relations(rel, path=path + '/RecordRelation')
                desc_elements.append(crelation)

    if record.find(s2_ns('StorageLocation')) is not None:
        storage = ead3.ead_element(tag='p',
                                   contents=record.find(
                                       s2_ns('StorageLocation')).text)
        originalsloc = ead3.ead_wrapper(tag='originalsloc',
                                        contents=[storage],
                                        encodinganalog=path +
                                        '/StorageLocation')
        desc_elements.append(originalsloc)

    if record.find(s2_ns('SignatureDescription')) is not None:
        signdesc = ead3.ead_element(tag='p',
                                    contents=record.find(
                                        s2_ns('SignatureDescription')).text)
        signdescwrap = ead3.ead_wrapper(tag='processinfo',
                                        contents=[signdesc],
                                        encodinganalog=path +
                                        '/SignatureDescription')
        desc_elements.append(signdescwrap)

    if record.find(s2_ns('Authenticity')) is not None:
        auth = s2_authenticity(record.find(s2_ns('Authenticity')),
                               path=path + '/Authenticity')
        desc_elements.append(auth)

    if record.find(s2_ns('Coverage')) is not None:
        coverage = s2_coverage(record.find(s2_ns('Coverage')),
                               path=path + '/Coverage')
        desc_elements.append(coverage)

    if record.find(s2_ns('Audience')) is not None:
        for elem in record.findall(s2_ns('Audience')):
            elem = ead3.ead_element(tag='p', contents=elem.text)
            audiences.append(elem)
        audience = ead3.ead_wrapper(tag='scopecontent', contents=audiences,
                                    encodinganalog=path + '/Audience')
        desc_elements.append(audience)

    if record.find(s2_ns('Source')) is not None:
        for elem in record.findall(s2_ns('Source')):
            elem = ead3.ead_element(tag='p', contents=elem.text)
            sources.append(elem)
        source = ead3.ead_wrapper(tag='relatedmaterial', contents=sources,
                                  encodinganalog=path + '/Source')
        desc_elements.append(source)

    if record.find(s2_ns('Rights')) is not None:
        for elem in record.findall(s2_ns('Rights')):
            elem = ead3.ead_element(tag='p', contents=elem.text)
            rightslist.append(elem)
        rights = ead3.ead_wrapper(tag='userestrict', contents=rightslist,
                                  encodinganalog=path + '/Rights')
        desc_elements.append(rights)

    if record.find(s2_ns('Version')) is not None:
        version = ead3.ead_element(tag='p',
                                   contents=record.find(s2_ns('Version')).text)
        versionwrap = ead3.ead_wrapper(tag='processinfo', contents=[version],
                                       encodinganalog=path + '/Version')
        desc_elements.append(versionwrap)

    if record.find(s2_ns('ProtectionClass')) is not None:
        for elem in record.findall(s2_ns('ProtectionClass')):
            elem = ead3.ead_element(tag='p', contents=elem.text)
            protlist.append(elem)
        protclass = ead3.ead_wrapper(tag='phystech', contents=protlist,
                                     encodinganalog=path + '/ProtectionClass')
        desc_elements.append(protclass)

    if record.find(s2_ns('Document')) is not None:
        for document in record.findall(s2_ns('Document')):
            document = create_c04(document)
            csubs.append(document)

    ead3_c = ead3.ead_c(cnum='c03', did=create_cdid(record,
                                                    path=path),
                        desc_elements=desc_elements,
                        csubs=csubs,
                        level='otherlevel',
                        otherlevel='Record',
                        encodinganalog=path)

    return ead3_c


def create_c04(document):
    """Creates the c04 element that corresponds to the SÄHKE2
    Document element.
    """
    path = '/Metadata/CaseFile/Action/Record/Document'
    desc_elemlist = desc_elems(document, path=path)
    desc_elements = []
    hashalgorithm = document.find(s2_ns('HashAlgorithm')).text
    hash_value = document.find(s2_ns('HashValue')).text

    for item in desc_elemlist:
        desc_elements.append(item)

    if document.find(s2_ns('UseType')) is not None:
        usetype_p = ead3.ead_element(tag='p',
                                     contents=document.find(
                                         s2_ns('UseType')).text)
        usetype = ead3.ead_wrapper(tag='scopecontent', contents=[usetype_p],
                                   encodinganalog=path + '/UseType')
        desc_elements.append(usetype)

    if document.find(s2_ns('HashAlgorithm')) is not None:
        hashalg = ead3.ead_element(tag='p',
                                   contents=hashalgorithm)
        hashalg_proc = ead3.ead_wrapper(tag='processinfo', contents=[hashalg],
                                        encodinganalog=path + '/HashAlgorithm')
        desc_elements.append(hashalg_proc)

    if document.find(s2_ns('HashValue')) is not None:
        hashvalue = ead3.ead_element(tag='p',
                                     contents=hash_value)
        hashsum = ead3.ead_wrapper(tag='processinfo', contents=[hashvalue],
                                   encodinganalog=path + '/HashValue')
        desc_elements.append(hashsum)

    if document.find(s2_ns('Encryption')) is not None:
        encryption = ead3.ead_element(tag='p',
                                      contents=document.find(
                                          s2_ns('Encryption')).text)
        encrypt = ead3.ead_wrapper(tag='processinfo', contents=[encryption],
                                   encodinganalog=path + '/Encryption')
        desc_elements.append(encrypt)

    ead4_c = ead3.ead_c(cnum='c04', did=create_cdid(document, path=path),
                        desc_elements=desc_elements, level='otherlevel',
                        otherlevel='Document', encodinganalog=path)

    return ead4_c


def desc_elems(parent, path):
    """All descriptive elements within a c element that's not part of
    the did element are created here and appended to the desc_elements
    list.
    """
    descriptions = []
    abstracts = []
    subjects = []
    desc_elements = []
    agents = []

    if parent.find(s2_ns('Abstract')) is not None:
        for abstract in parent.findall(s2_ns('Abstract')):
            abstract = ead3.ead_element(tag='p',
                                        contents=abstract.text)
            abstracts.append(abstract)
        abstr_scopecontent = ead3.ead_wrapper(tag='scopecontent',
                                              contents=abstracts,
                                              encodinganalog=path +
                                              '/Abstract')
        desc_elements.append(abstr_scopecontent)

    if parent.find(s2_ns('Description')) is not None:
        for description in parent.findall(s2_ns('Description')):
            description = ead3.ead_element(tag='p', contents=description.text)
            descriptions.append(description)
        desc_scopecontent = ead3.ead_wrapper(tag='scopecontent',
                                             contents=descriptions,
                                             encodinganalog=path +
                                             '/Description')
        desc_elements.append(desc_scopecontent)

    if parent.find(s2_ns('Subject')) is not None:
        for subject in parent.findall(s2_ns('Subject')):
            subject = ead3.ead_element(tag='part', contents=subject.text)
            sub = ead3.ead_wrapper(tag='subject', contents=[subject])
            subjects.append(sub)
        desc_subject = ead3.ead_wrapper(tag='controlaccess',
                                        contents=subjects,
                                        encodinganalog=path + '/Subject')
        desc_elements.append(desc_subject)

    if parent.find(s2_ns('Type')) is not None:
        stype = ead3.ead_element(tag='part',
                                 contents=parent.find(s2_ns('Type')).text)
        genref_stype = ead3.ead_wrapper(tag='genreform',
                                        contents=[stype])
        desc_stype = ead3.ead_wrapper(tag='controlaccess',
                                      contents=[genref_stype],
                                      encodinganalog=path + '/Type')
        desc_elements.append(desc_stype)

    if parent.find(s2_ns('Function')) is not None:
        func_part = ead3.ead_element(tag='part',
                                     contents=parent.find(
                                         s2_ns('Function')).text)
        function = ead3.ead_wrapper(tag='function', contents=[func_part])
        func_ctrlacc = ead3.ead_wrapper(tag='controlaccess',
                                        contents=[function],
                                        encodinganalog=path + '/Function')
        desc_elements.append(func_ctrlacc)

    if parent.find(s2_ns('Status')) is not None:
        status_p = ead3.ead_element(tag='p',
                                    contents=parent.find(s2_ns('Status')).text)
        status = ead3.ead_wrapper(tag='processinfo', contents=[status_p],
                                  encodinganalog=path + '/Status')
        desc_elements.append(status)

    if parent.find(s2_ns('Custom')) is not None:
        custom = ET.Element('Custom')
        for elem in parent.find(s2_ns('Custom')):
            custom.append(elem)
        xmlwrap = ead3.ead_wrapper(tag='objectxmlwrap', contents=[custom])
        relation = ead3.ead_wrapper(tag='relation', contents=[xmlwrap],
                                    relationtype='otherrelationtype',
                                    otherrelationtype='otherXML')
        relations = ead3.ead_wrapper(tag='relations', contents=[relation],
                                     encodinganalog=path + '/Custom')
        desc_elements.append(relations)

    if parent.find(s2_ns('Agent')) is not None:
        for agent in parent.findall(s2_ns('Agent')):
            eadagent = s2_agent(agent, path=path)
            agents.append(eadagent)
        agent_p = ead3.ead_wrapper(tag='p', contents=agents)
        agent_controlacc = ead3.ead_wrapper(tag='controlaccess',
                                            contents=[agent_p],
                                            encodinganalog=path + '/Agent')
        desc_elements.append(agent_controlacc)

    return desc_elements


def create_dao(parent, path):
    """Creates dao from SÄHKE2 file elements. File.Name and File.Path
    are converted into the descriptivenote element within dao.
    File.Path is also used to create the @href attribute.
    """
    parts = []

    filename = ead3.ead_element(tag='part',
                                contents=parent.find(s2_ns('Name')).text,
                                encodinganalog=path + '/Name')
    parts.append(filename)

    filepath = ead3.ead_element(tag='part',
                                contents=parent.find(s2_ns('Path')).text,
                                encodinganalog=path + '/Path')
    parts.append(filepath)

    name = ead3.ead_wrapper(tag='name', contents=parts)
    dao_p = ead3.ead_wrapper(tag='p', contents=[name])

    descnote = ead3.ead_wrapper(tag='descriptivenote', contents=[dao_p])

    dao = ead3.ead_wrapper(tag='dao', contents=[descnote],
                           encodinganalog=path)

    dao.set('daotype', 'otherdaotype')
    dao.set('otherdaotype', u'Sähke2-File')
    dao.set('href', parent.find(s2_ns('Path')).text.replace('\\', '/'))

    return dao


def s2_contactinformation(parent, path):
    """Creates repository elements from SÄHKE2 ContactInformation.
    Persons are converteed into the persname/part element, while
    organisations are converted into the corpname/part element.
    """
    contents = []
    persnames = []

    org_part = ead3.ead_element(tag='part',
                                contents=parent.find(s2_ns('Organisation',
                                                           child='Name')).text)
    org = ead3.ead_wrapper(tag='corpname', contents=[org_part],
                           encodinganalog=path + '/Organisation/Name')
    contents.append(org)

    for elem in parent.findall(s2_ns('ContactPerson', child='*')):
        tag = ET.QName(elem.tag).localname
        epath = path + '/ContactPerson'
        elem = ead3.ead_element(tag='part', contents=elem.text,
                                encodinganalog=epath + '/' + tag)
        persnames.append(elem)
    persname = ead3.ead_wrapper(tag='persname', contents=persnames,
                                encodinganalog=epath)
    contents.append(persname)

    repository = ead3.ead_wrapper(tag='repository', contents=contents,
                                  encodinganalog=path)

    return repository


def s2_authenticity(parent, path):
    """Creates processinfo elements from SÄHKE2 Authenticity, with
    names and dates as respective elements.
    """
    contents = []

    checker = ead3.ead_element(tag='part',
                               contents=parent.find(s2_ns('Checker')).text,
                               encodinganalog=path + '/Checker')
    checkername = ead3.ead_wrapper(tag='name', contents=[checker])
    checkerp = ead3.ead_wrapper(tag='p', contents=[checkername])
    checker_proc = ead3.ead_wrapper(tag='processinfo', contents=[checkerp],
                                    encodinganalog=path + '/Checker')
    contents.append(checker_proc)

    date = ead3.ead_element(tag='date',
                            contents=parent.find(s2_ns('Date')).text,
                            encodinganalog=path + '/Date')
    datep = ead3.ead_wrapper(tag='p', contents=[date])
    date_proc = ead3.ead_wrapper(tag='processinfo', contents=[datep],
                                 encodinganalog=path + '/Date')
    contents.append(date_proc)

    desc = ead3.ead_element(tag='p',
                            contents=parent.find(s2_ns('Description')).text)
    desc_proc = ead3.ead_wrapper(tag='processinfo', contents=[desc],
                                 encodinganalog=path + '/Description')
    contents.append(desc_proc)

    processinfo = ead3.ead_wrapper(tag='processinfo', contents=contents,
                                   encodinganalog=path)

    return processinfo


def s2_classlevel(parent, path):
    """Creates an hierarchical list from SÄHKE2 ClassificationScheme
    with Title and FunctionCode as item/part elements. Each level of
    SubFunctions is created as a list within an item element.
    """
    items = []

    functions = []

    title = ead3.ead_element(tag='part',
                             contents=parent.find(s2_ns('Title')).text,
                             encodinganalog=path + '/Title')
    functions.append(title)

    code = ead3.ead_element(tag='part',
                            contents=parent.find(s2_ns('FunctionCode')).text,
                            encodinganalog=path + '/FunctionCode')
    functions.append(code)

    function = ead3.ead_wrapper(tag='function', contents=functions,
                                encodinganalog=path)
    item = ead3.ead_wrapper(tag='item', contents=[function])
    items.append(item)

    if parent.find(s2_ns('FunctionClassification')) is not None:
        subfunc = s2_classlevel(parent.find(s2_ns('FunctionClassification')),
                                path=path + '/FunctionClassification')
        item = ead3.ead_wrapper(tag='item', contents=[subfunc])
        items.append(item)
    elif parent.find(s2_ns('SubFunction')) is not None:
        subfunction = s2_classlevel(parent.find(s2_ns('SubFunction')),
                                    path=path + '/SubFunction')
        item = ead3.ead_wrapper(tag='item', contents=[subfunction])
        items.append(item)

    funclist = ead3.ead_wrapper(tag='list', contents=items)

    return funclist


def s2_restriction(restriction, path):
    """Creates accessrestrict/p elements from SÄHKE2 Restriction,
    wrapped inside an outer accessrestrict element. Dates are
    included as date elements. Restriction elements can occur in
    several hierarchies inside a SÄHKE2 document.
    """
    contents = []

    if restriction.find(s2_ns('PublicityClass')) is not None:
        elem = ead3.ead_element(tag='p',
                                contents=restriction.find(
                                    s2_ns('PublicityClass')).text)
        pubclass = ead3.ead_wrapper(tag='accessrestrict', contents=[elem],
                                    encodinganalog=path + '/PublicityClass')
        contents.append(pubclass)

    if restriction.find(s2_ns('SecurityPeriod')) is not None:
        elem = ead3.ead_element(tag='p',
                                contents=restriction.find(
                                    s2_ns('SecurityPeriod')).text)
        secuperiod = ead3.ead_wrapper(tag='accessrestrict', contents=[elem],
                                      encodinganalog=path + '/SecurityPeriod')
        contents.append(secuperiod)

    if restriction.find(s2_ns('SecurityPeriodEnd')) is not None:
        secuperiodend = ead3.ead_element(tag='date',
                                         contents=restriction.find(
                                             s2_ns('SecurityPeriodEnd')).text,
                                         encodinganalog=path +
                                         '/SecurityPeriodEnd')
        secuperiodend_p = ead3.ead_wrapper(tag='p', contents=[secuperiodend])
        secuperiodend_c = ead3.ead_wrapper(tag='accessrestrict',
                                           contents=[secuperiodend_p],
                                           encodinganalog=path +
                                           '/SecurityPeriodEnd')
        contents.append(secuperiodend_c)

    if restriction.find(s2_ns('SecurityReason')) is not None:
        elem = ead3.ead_element(
            tag='p',
            contents=restriction.find(s2_ns('SecurityReason')).text)
        secureason = ead3.ead_wrapper(tag='accessrestrict', contents=[elem],
                                      encodinganalog=path + '/SecurityReason')
        contents.append(secureason)

    if restriction.find(s2_ns('ProtectionLevel')) is not None:
        elem = ead3.ead_element(
            tag='p',
            contents=restriction.find(s2_ns('ProtectionLevel')).text)
        protlevel = ead3.ead_wrapper(tag='accessrestrict', contents=[elem],
                                     encodinganalog=path + '/ProtectionLevel')
        contents.append(protlevel)

    if restriction.find(s2_ns('SecurityClass')) is not None:
        elem = ead3.ead_element(
            tag='p',
            contents=restriction.find(s2_ns('SecurityClass')).text)
        secuclass = ead3.ead_wrapper(tag='accessrestrict', contents=[elem],
                                     encodinganalog=path + '/SecurityClass')
        contents.append(secuclass)

    if restriction.find(s2_ns('PersonalData')) is not None:
        elem = ead3.ead_element(
            tag='p',
            contents=restriction.find(s2_ns('PersonalData')).text)
        persdata = ead3.ead_wrapper(tag='accessrestrict', contents=[elem],
                                    encodinganalog=path + '/PersonalData')
        contents.append(persdata)

    if restriction.find(s2_ns('Person')) is not None and len(
            restriction.find(s2_ns('Person'))):
        for person in restriction.findall(s2_ns('Person')):
            p_data = []
            if person.find(s2_ns('Name')) is not None:
                part_name = ead3.ead_element(tag='part',
                                             contents=person.find(
                                                 s2_ns('Name')).text,
                                             encodinganalog=path +
                                             '/Person/Name')
                p_data.append(part_name)
            if person.find(s2_ns('Ssn')) is not None:
                ssn = ead3.ead_element(tag='part',
                                       contents=person.find(s2_ns('Ssn')).text,
                                       encodinganalog=path + '/Person/Ssn')
                p_data.append(ssn)
            if person.find(s2_ns('ElectronicId')) is not None:
                eid = ead3.ead_element(tag='part',
                                       contents=person.find(
                                           s2_ns('ElectronicId')).text,
                                       encodinganalog=path +
                                       '/Person/ElectronicId')
                p_data.append(eid)
            persname = ead3.ead_wrapper(tag='name', contents=p_data)
            person = ead3.ead_wrapper(tag='p',
                                      contents=[persname])
            pers = ead3.ead_wrapper(tag='accessrestrict', contents=[person],
                                    encodinganalog=path + '/Person')
            contents.append(pers)

    if restriction.find(s2_ns('Owner')) is not None:
        for owner in restriction.findall(s2_ns('Owner')):
            own_elem = ead3.ead_element(tag='part',
                                        contents=owner.text,
                                        encodinganalog=path + '/Owner')
            name_owner = ead3.ead_wrapper(tag='name', contents=[own_elem])
            p_owner = ead3.ead_wrapper(tag='p', contents=[name_owner])
            pers = ead3.ead_wrapper(tag='accessrestrict', contents=[p_owner],
                                    encodinganalog=path + '/Owner')
            contents.append(pers)

    if restriction.find(s2_ns('AccessRight')) is not None:
        for rightsaccess in restriction.findall(s2_ns('AccessRight')):
            p_data = []
            part_name = ead3.ead_element(tag='part',
                                         contents=rightsaccess.find(
                                             s2_ns('Name')).text,
                                         encodinganalog=path +
                                         '/AccessRight/Name')
            p_data.append(part_name)
            role = ead3.ead_element(tag='part',
                                    contents=rightsaccess.find(
                                        s2_ns('Role')).text,
                                    encodinganalog=path + '/AccessRight/Role')
            p_data.append(role)
            if rightsaccess.find(s2_ns('AccessRightDescription')) is not None:
                rights_desc = ead3.ead_element(
                    tag='part',
                    contents=rightsaccess.find(
                        s2_ns('AccessRightDescription')).text,
                    encodinganalog=path +
                    '/AccessRight/AccessRightDescription')
                p_data.append(rights_desc)
            name = ead3.ead_wrapper(tag='name', contents=p_data)
            p_name = ead3.ead_wrapper(tag='p', contents=[name])
            pers = ead3.ead_wrapper(tag='accessrestrict', contents=[p_name],
                                    encodinganalog=path + '/AccessRight')
            contents.append(pers)

    accrestrict = ead3.ead_wrapper(tag='accessrestrict',
                                   contents=contents, encodinganalog=path)

    return accrestrict


def s2_agent(agent, path):
    """Converts SÄHKE2 Agent into name/part elements. The role of the
    agent is converted into the @relator attribute if it is part of a
    controlled vocabulary, but it is also as a part element of its own.
    """
    contents = []

    roles = ['Laatija', 'Toimeksiantaja', u'Esittelijä', 'Kirjaaja',
             'Ratkaisija', 'Valmistelija', u'Vastuuhenkilö', 'Vastuutaho',
             'Julkaisija', u'Lähettäjä', u'Muu tekijä', 'Vastaanottaja',
             'Jakelija']

    if agent.find(s2_ns('Role')) is not None:
        roleattr = agent.find(s2_ns('Role')).text
        role = ead3.ead_element(tag='part',
                                contents=agent.find(s2_ns('Role')).text,
                                encodinganalog=path + '/Agent/Role')
        contents.append(role)

    if agent.find(s2_ns('Name')) is not None:
        name = ead3.ead_element(tag='part',
                                contents=agent.find(s2_ns('Name')).text,
                                encodinganalog=path + '/Agent/Name')
        contents.append(name)

    if agent.find(s2_ns('CorporateName')) is not None:
        corpname = ead3.ead_element(tag='part',
                                    contents=agent.find(
                                        s2_ns('CorporateName')).text,
                                    encodinganalog=path +
                                    '/Agent/CorporateName')
        contents.append(corpname)

    agent = ead3.ead_wrapper(tag='name', contents=contents)

    if roleattr in roles:
        agent.set('relator', roleattr)

    return agent


def s2_coverage(parent, path):
    """Converts SÄHKE2 Coverage elements (Jurisdiction, Spatial,
    Temporal) into controlaccess as subject/part, geogname/part
    and p/date elements respectively.
    """
    contents = []

    if parent.find(s2_ns('Jurisdiction')) is not None:
        for elem in parent.findall(s2_ns('Jurisdiction')):
            elem = ead3.ead_element(tag='part',
                                    contents=elem.text)
            subject = ead3.ead_wrapper(tag='subject', contents=[elem],
                                       encodinganalog=path + '/Jurisdiction')
            contents.append(subject)

    if parent.find(s2_ns('Spatial')) is not None:
        for elem in parent.findall(s2_ns('Spatial')):
            elem = ead3.ead_element(tag='part',
                                    contents=elem.text)
            geogname = ead3.ead_wrapper(tag='geogname', contents=[elem],
                                        encodinganalog=path + '/Spatial')
            contents.append(geogname)

    if parent.find(s2_ns('Temporal')) is not None:
        for elem in parent.findall(s2_ns('Temporal')):
            elem = ead3.ead_element(tag='date',
                                    contents=elem.text,
                                    encodinganalog=path + '/Temporal')
            date = ead3.ead_wrapper(tag='p', contents=[elem])
            contents.append(date)

    controlaccess = ead3.ead_wrapper(tag='controlaccess', contents=contents,
                                     encodinganalog=path)

    return controlaccess


def s2_electronicnotif(enotif, path):
    """Converts SÄHKE2 ElectronicNotification into processinfo,
    with all SÄHKE2 elements as processinfo/p/date elements,except for
    the AcceptionDescription element that is text and doesn't contain
    a date element.
    """
    epath = path + '/ElectronicNotification'
    items = []

    for elem in enotif.findall('*'):
        tag = ET.QName(elem.tag).localname
        if tag == 'AcceptionDescription':
            elem = ead3.ead_element(tag='p', contents=elem.text)
            acc_desc = ead3.ead_wrapper(tag='processinfo', contents=[elem],
                                        encodinganalog=epath + '/' + tag)
            items.append(acc_desc)
        else:
            elem = ead3.ead_element(tag='date', contents=elem.text)
            date_item = ead3.ead_wrapper(tag='p', contents=[elem])
            date = ead3.ead_wrapper(tag='processinfo', contents=[date_item],
                                    encodinganalog=epath + '/' + tag)
            items.append(date)

    processinfo = ead3.ead_wrapper(tag='processinfo', contents=items,
                                   encodinganalog=epath)

    return processinfo


def s2_relations(relation, path):
    """Converts SÄHKE2 Relations, both CaseFileRelation and
    RecordRelation, into EAD3 relatedmaterial/p elements.
    """
    encodinganalog = path + '/' + ET.QName(relation.tag).localname

    elem = ead3.ead_element(tag='p', contents=relation.text)

    relatedmaterial = ead3.ead_wrapper(tag='relatedmaterial',
                                       contents=[elem],
                                       encodinganalog=encodinganalog)

    return relatedmaterial


if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)

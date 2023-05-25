Configuration
-------------
All configuration in default configuration file: `/etc/siptools_research.conf`.

packaging_root
    Directory where workspaces are created for each workflow. Default: ``"/var/spool/siptools-research"``
mongodb_host
    MongoDB server host. Default: ``"localhost"``
mongodb_database
    MongoDB database name. Default: ``"siptools-research"``
mongodb_collection
    MongoDB collection name. Default: ``"workflow"``
metax_url
    Metax url. Default: ``"https://metax-test.csc.fi"``
metax_user
    Metax username. Default: ``"tpas"``
metax_password
    Metax password. Default: ``""``
ida_token
    Ida token. Default: ``""``
dp_host
    Digital preservation service host. Default: ``"86.50.168.218"``
dp_user
    Digital preservation service username. Default: ``"tpas"``
dp_ssh_key
    Path to private SSH key for authentication to digital preservation service. Default: ``"~/.ssh/id_rsa_tpas_pouta"``
sip_sign_key
    Private key used for signing SIP. Default: ``"~/sip_sign_pas.pem"``

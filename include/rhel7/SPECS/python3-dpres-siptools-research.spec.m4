# vim:ft=spec

%define file_prefix M4_FILE_PREFIX
%define file_ext M4_FILE_EXT
%define file_version M4_FILE_VERSION
%define file_release_tag %{nil}M4_FILE_RELEASE_TAG
%define file_release_number M4_FILE_RELEASE_NUMBER
%define file_build_number M4_FILE_BUILD_NUMBER
%define file_commit_ref M4_FILE_COMMIT_REF

%define user_name siptools_research
%define user_group siptools_research
%define user_gid 335
%define user_uid 335

Name:           python3-dpres-siptools-research
Version:        %{file_version}
Release:        %{file_release_number}%{file_release_tag}.%{file_build_number}.git%{file_commit_ref}%{?dist}
Summary:        Digital Preservation Packaging Service for Research datasets
Group:          Applications/Archiving
License:        LGPLv3+
URL:            https://www.digitalpreservation.fi
Source0:        %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}.%{file_ext}
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch
Requires:       python3
Requires:       python3-setuptools
Requires:       python3-luigi
Requires:       python36-pymongo
Requires:       python3-dpres-siptools
Requires:       python3-mets
Requires:       python36-lxml
Requires:       python36-paramiko
Requires:       python36-jsonschema
Requires:       python36-requests
Requires:       python3-iso-639
Requires:       ImageMagick
Requires:       python36-dateutil
Requires:       dpres-xml-schemas
Requires:       python3-metax-access
Requires:       python3-file-scraper-full
Requires:       python3-upload-rest-api
Requires:       logrotate
BuildRequires:  python3-setuptools
BuildRequires:  python36-setuptools_scm
BuildRequires:  python36-pytest
BuildRequires:  python3-sphinx
BuildRequires:  python3-jsonschema2rst
BuildRequires:  python36-xmltodict
BuildRequires:  python3-mongomock
BuildRequires:  python36-pytest-catchlog
BuildRequires:  python3-requests-mock
BuildRequires:  python3-mock-ssh-server
BuildRequires:  python3-pytest-mock

%description
Digital Preservation Packaging Service for Research datasets


%prep
%setup -n %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}


%build

%pre
getent group %{user_group} >/dev/null || groupadd -f -g %{user_gid} -r %{user_group}
if ! getent passwd %{user_name} >/dev/null ; then
    if ! getent passwd %{user_uid} >/dev/null ; then
      useradd -r -m -K UMASK=0027 -u %{user_uid} -g %{user_group} -s /sbin/nologin -c "SIP Tools Research user" -d /var/lib/%{user_name} %{user_name}
    else
      useradd -r -g %{user_group} -s /sbin/nologin -c "SIP Tools Research group" %{user_name}
    fi
fi

usermod -aG %{user_group} %{user_name}


%install
rm -rf $RPM_BUILD_ROOT
make install PREFIX="%{_prefix}" DESTDIR="%{buildroot}" SETUPTOOLS_SCM_PRETEND_VERSION=%{file_version}
mkdir -p %{buildroot}/var/spool/siptools-research

# Rename executable to prevent naming collision with Python 2 RPM
sed -i 's/\/bin\/siptools-research$/\/bin\/siptools-research-3/g' INSTALLED_FILES

mv %{buildroot}%{_bindir}/siptools-research %{buildroot}%{_bindir}/siptools-research-3

%post
chown %{user_name}:%{user_group} /var/lib/%{user_name}
chmod 770 /var/lib/%{user_name}


%clean
rm -rf $RPM_BUILD_ROOT


%files -f INSTALLED_FILES
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/siptools_research.conf
%config %{_sysconfdir}/luigi/research_logging.cfg
%config(noreplace) %{_sysconfdir}/logrotate.d/siptools_research
%attr(-,siptools_research,siptools_research) /var/log/siptools_research
%attr(-,siptools_research,siptools_research) /var/spool/siptools_research
/usr/lib/systemd/system/siptools_research-3.service
/usr/lib/systemd/system/siptools_research-3.timer


# TODO: For now changelog must be last, because it is generated automatically
# from git log command. Appending should be fixed to happen only after %changelog macro
%changelog

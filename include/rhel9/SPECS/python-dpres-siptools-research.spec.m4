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

Name:           python-dpres-siptools-research
Version:        %{file_version}
Release:        %{file_release_number}%{file_release_tag}.%{file_build_number}.git%{file_commit_ref}%{?dist}
Summary:        Digital Preservation Packaging Service for Research datasets
License:        LGPLv3+
URL:            https://www.digitalpreservation.fi
Source0:        %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}.%{file_ext}
BuildArch:      noarch

BuildRequires:  python3-devel
BuildRequires:  pyproject-rpm-macros
BuildRequires:  %{py3_dist jsonschema2rst}
BuildRequires:  %{py3_dist metax-access}
BuildRequires:  %{py3_dist mock-ssh-server}
BuildRequires:  %{py3_dist mongobox}
BuildRequires:  %{py3_dist mongoengine}
BuildRequires:  %{py3_dist mongomock}
BuildRequires:  %{py3_dist pip}
BuildRequires:  %{py3_dist pytest}
BuildRequires:  %{py3_dist pytest-mock}
BuildRequires:  %{py3_dist requests_mock}
BuildRequires:  %{py3_dist setuptools}
BuildRequires:  %{py3_dist setuptools_scm}
BuildRequires:  %{py3_dist wheel}

%global _description %{expand:
Digital Preservation Packaging Service for Research datasets
}

%description %_description

%package -n python3-dpres-siptools-research
Summary:  %{summary}
Requires: %{py3_dist file-scraper}
Requires: %{py3_dist siptools}
Requires: %{py3_dist metax-access}
Requires: %{py3_dist upload_rest_api}
Requires: dpres-xml-schemas

%description -n python3-dpres-siptools-research %_description

%prep
%autosetup -n %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}

%build
export SETUPTOOLS_SCM_PRETEND_VERSION=%{file_version}
%pyproject_wheel

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
%pyproject_install
%pyproject_save_files siptools_research

# Copy config file to /etc/siptools_research.conf with correct permissions
install -D -m 0644 include/etc/siptools_research.conf %{buildroot}%{_sysconfdir}/siptools_research.conf
install -D -m 0644 include/etc/logrotate.d/siptools_research %{buildroot}%{_sysconfdir}/logrotate.d/siptools_research

# TODO: executables with "-3" suffix are added to maintain compatibility with our systems.
# executables with "-3" suffix should be deprecated.
cp %{buildroot}%{_bindir}/siptools-research %{buildroot}%{_bindir}/siptools-research-3

%post
chown %{user_name}:%{user_group} /var/lib/%{user_name}
chmod 770 /var/lib/%{user_name}

%files -n python3-dpres-siptools-research -f %{pyproject_files}
%{_bindir}/siptools-research
%{_bindir}/siptools-research-3
%config(noreplace) %{_sysconfdir}/siptools_research.conf
%config(noreplace) %{_sysconfdir}/logrotate.d/siptools_research
%config %{_sysconfdir}/luigi/research_logging.cfg

# TODO: For now changelog must be last, because it is generated automatically
# from git log command. Appending should be fixed to happen only after %changelog macro
%changelog
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

Name:           dpres-siptools-research
Version:        %{file_version}
Release:        %{file_release_number}%{file_release_tag}.%{file_build_number}.git%{file_commit_ref}%{?dist}
Summary:        Workflow to create KDK-PAS compatible SIP packages from SAHKE2 packages
Group:          Applications/Archiving
License:        LGPLv3+
URL:            http://www.csc.fi
Source0:        %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}.%{file_ext}
Source1:        siptools_research.sudoers
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch

Requires:       python python-setuptools python-luigi python-scandir
Requires:       python-lxml python-wand
Requires:       dpres-ipt dpres-siptools
Requires:       cronie logrotate sudo zip
BuildRequires:  python-setuptools pytest cronie

%description
Workflow to create KDK-PAS compatible SIP packages from SAHKE2 packages

%prep
%setup -n %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}

%build

%pre
getent group %{user_group} >/dev/null || groupadd -f -g %{user_gid} -r %{user_group}
if ! getent passwd %{user_name} >/dev/null ; then
    if ! getent passwd %{user_uid} >/dev/null ; then
      useradd -r -m -K UMASK=0027 -u %{user_uid} -g %{user_group} -s
/sbin/nologin -c "SIP Tools Research user" -d /var/lib/%{user_name} %{user_name}
    else
      useradd -r -g %{user_group} -s /sbin/nologin -c "SIP Tools Research group" %{user_name}
    fi
fi

usermod -aG %{user_group} %{user_name}


%install
rm -rf $RPM_BUILD_ROOT
install -p -m 0440 %{SOURCE1} -D %{buildroot}/etc/sudoers.d/siptools_research
make install PREFIX="%{_prefix}" ROOT="%{buildroot}"
chmod 755 %{buildroot}/usr/share/siptools_research/*.sh
mkdir -p  %{buildroot}/var/lib/siptools_research



%post
chown %{user_name}:%{user_group} /var/lib/%{user_name}
chmod 770 /var/lib/%{user_name}

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root,-)
%config(noreplace) /etc/logrotate.d/*
%config(noreplace) /etc/siptools_research/*
%config /etc/sudoers.d/siptools_research
/usr/share/siptools_research/*.sh
%attr(-,siptools_research,siptools_research) /var/log/siptools_research
%attr(-,siptools_research,siptools_research) /var/spool/siptools_research
%attr(770,siptools_research,siptools_research) /var/lib/siptools_research

# TODO: For now changelog must be last, because it is generated automatically
# from git log command. Appending should be fixed to happen only after %changelog macro
%changelog

#!/bin/bash

EXIT_STATUS=0
GROUP=sftpusers
CHROOT_HOME=/home
INGEST_VOL=/mnt/ingest_vol/home
WORKSPACE_DIR=/mnt/ingest_vol/var/spool/siptools_research

if [ $# -ne 2 ]
then
    echo "Siptools-research SFTP user management tool"
    echo "Usage: siptools_research_user adduser/lockuser/deleteuser username"
    exit 1
fi

COMMAND=$1
USERNAME=$2

function adduser {
    echo "Adding user ${USERNAME}"

    if getent passwd ${USERNAME}
    then
        echo "User ${USERNAME} already exists, attempting to reset permissions."
    fi

    if ! getent group siptools_research
    then
        echo -n "The group siptools_research does not exist on the system, this "
        echo "could be a configuration problem, quitting!"
        exit 1
    fi

    if ! getent passwd nginx
    then
        echo "Web server user nginx doesn't exist, creating user."
        groupadd -g 9905 nginx
        useradd -g nginx -d /var/lib/nginx -u 9904 -s /sbin/nologin nginx
    else
        NGINX_ID=$(id -u nginx)
        if [ "$NGINX_ID" -ne "9904" ]
        then
            echo "nginx id not 9904, updating user and group id."
            service nginx stop
            usermod -u 9904 nginx
            groupmod -g 9905 nginx
            service nginx start
        fi
    fi

    if ! getent group ${GROUP}
    then
        groupadd -g 901 ${GROUP}
    fi

    if ! getent group ${USERNAME}
    then
        groupadd ${USERNAME}
    fi

    if [ -d ${INGEST_VOL} ]
    then
        CHROOT_HOME=${INGEST_VOL}
    fi

    mkdir -p ${CHROOT_HOME}/${USERNAME}
    chown root:${USERNAME} ${CHROOT_HOME}/${USERNAME} || EXIT_STATUS=$?

    useradd -g ${GROUP} -d ${CHROOT_HOME}/${USERNAME} -s /sbin/nologin ${USERNAME} || EXIT_STATUS=$?
    # useradd exit status 9 means the user name already exists, so we reset the
    # exit status
    if [ $EXIT_STATUS -eq 9 ]
    then
        EXIT_STATUS=0
    fi
    passwd -l ${USERNAME} || EXIT_STATUS=$?

    chmod 751 ${CHROOT_HOME}/${USERNAME} || EXIT_STATUS=$?

    for dir in transfer accepted rejected reports ; do
		[ -d "${CHROOT_HOME}/${USERNAME}/$dir" ] || mkdir -p "${CHROOT_HOME}/${USERNAME}/$dir"
		chown "${USERNAME}:siptools_research" "${CHROOT_HOME}/${USERNAME}/$dir"
		chmod 770 "${CHROOT_HOME}/${USERNAME}/$dir"
		chmod g+s "${CHROOT_HOME}/${USERNAME}/$dir"
        chmod 660 -R "${CHROOT_HOME}/${USERNAME}/$dir/*"
    done

    [ -d "${CHROOT_HOME}/${USERNAME}/.ssh" ] || mkdir -p "${CHROOT_HOME}/${USERNAME}/.ssh"
    chmod 700 "${CHROOT_HOME}/${USERNAME}/.ssh"
    chown "${USERNAME}:${USERNAME}" "${CHROOT_HOME}/${USERNAME}/.ssh"
    usermod -a -G ${USERNAME} siptools_research
    usermod -a -G ${USERNAME} ${USERNAME}

    if [ $EXIT_STATUS -eq 0 ]
    then
        fixgroups ${USERNAME}
    fi

}

function lockuser {
    if ! getent passwd ${USERNAME}
    then
        echo "User ${USERNAME} does not exist, quitting!"
        exit 1
    fi
    usermod -e 1970-01-01 ${USERNAME} || EXIT_STATUS=$?
}

function deleteuser {
    if ! getent passwd ${USERNAME}
    then
        echo "User ${USERNAME} does not exist, quitting!"
        exit 1
    fi

    echo "Attempting to remove user ${USERNAME}"
    userdel -r ${USERNAME} || EXIT_STATUS=$?
    echo "Attempting to remove group ${USERNAME}"
    groupdel ${USERNAME} || EXIT_STATUS=$?
}

function fixgroups {
    echo "Attempting to fix groups for user ${USERNAME}"
    ORG_GROUPS=$(id -Gn ${USERNAME})
    for ORG_GROUP in $ORG_GROUPS
    do
        echo "Checking group ${ORG_GROUP}"
        if [ "$ORG_GROUP" != "sftpusers" ] && [ "$ORG_GROUP" != "$USERNAME" ]
        then
            echo "Calling gpasswd to remove user ${USERNAME} from group ${ORG_GROUP}"
            gpasswd -d ${USERNAME} ${ORG_GROUP} || EXIT_STATUS=$?
        fi
    done
}

if [ "$COMMAND" = "adduser" ];
then
    adduser
elif [ "$COMMAND" = "lockuser" ];
then
    lockuser $USERNAME
elif [ "$COMMAND" = "deleteuser" ];
then
    deleteuser $USERNAME
else
    echo "Usage: siptools_research_user adduser/lockuser/deleteuser username"
    exit 1
fi


if [ $EXIT_STATUS -eq 0 ]
    then echo "Command succeeded"
elif [ "$COMMAND" != "deleteuser" ]
    then echo "It seems command has failed"
fi

exit $EXIT_STATUS

ROOT=/
PREFIX=/usr
ETC=${ROOT}/etc
SHAREDIR=${ROOT}${PREFIX}/share/siptools_research
CRONDIR=${ETC}/cron.d
VAR=/var

LOGDIR=${ROOT}${VAR}/log/siptools_research
LOGROTATEDIR=${ETC}/logrotate.d
LOGROTATEFILE=${LOGROTATEDIR}/siptools_research
PROCESSINGDIR=${ROOT}${VAR}/spool/siptools_research
LOGGINGCONFDIR=${ETC}/siptools_research

install:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Create log, share and processing directories
	mkdir -p "${LOGDIR}"
	mkdir -p "${PROCESSINGDIR}"
	mkdir -p "${LOGROTATEDIR}"
	mkdir -p "${SHAREDIR}"
	mkdir -p "${LOGGINGCONFDIR}"

	# Copy logrotate file to etc
	cp include/etc/logrotate.d/* ${LOGROTATEDIR}/
	chmod 644 ${LOGROTATEFILE}

	# Copy logging config file to etc
	cp include/etc/siptools_research/* ${LOGGINGCONFDIR}/
	chmod 644 ${LOGGINGCONFDIR}/logging.conf

	# Copy user management script to share
	cp include/share/siptools_research_user.sh "${SHAREDIR}"/
	chmod 644 ${SHAREDIR}/siptools_research_user.sh
	chmod +x ${SHAREDIR}/siptools_research_user.sh

	# Use Python setuptools
	python setup.py build ; python ./setup.py install -O1 --prefix="${PREFIX}" --root="${ROOT}" --record=INSTALLED_FILES
	cat INSTALLED_FILES | sed 's/^/\//g' >> INSTALLED_FILES

test:
	py.test -svvvv --junitprefix=dpres-siptools-research --junitxml=junit.xml tests

coverage:
	py.test tests --cov=siptools_research --cov-report=html
	coverage report -m
	coverage html
	coverage xml

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true

clean-rpm:
	rm -rf rpmbuild

rpm: clean
	create-archive.sh
	preprocess-spec-m4-macros.sh include/rhel7
	build-rpm.sh

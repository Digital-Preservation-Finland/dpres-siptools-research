DESTDIR ?= /
PREFIX ?= /usr
ETC = ${DESTDIR}/etc
SHAREDIR = ${DESTDIR}${PREFIX}/share/siptools_research
VAR = ${DESTDIR}/var
LIB = ${DESTDIR}${PREFIX}/lib

LOGDIR = ${VAR}/log/siptools_research
PROCESSINGDIR = ${VAR}/spool/siptools_research

PYTHON ?= python3

install:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Create log, share and processing directories
	mkdir -p "${LOGDIR}"
	mkdir -p "${PROCESSINGDIR}"
	mkdir -p "${SHAREDIR}"
	mkdir -p "${ETC}"
	mkdir -p "${LIB}"
	mkdir -p "${LIB}/systemd/system"

	# Copy config files
	cp include/etc/siptools_research.conf ${ETC}/
	install -D -m 0644 include/etc/luigi/research_logging.cfg ${ETC}/luigi/research_logging.cfg
	install -D -m 0644 include/etc/logrotate.d/siptools_research ${ETC}/logrotate.d/siptools_research
	cp include/usr/lib/systemd/system/siptools_research-3.service ${LIB}/systemd/system/siptools_research-3.service
	cp include/usr/lib/systemd/system/siptools_research.timer ${LIB}/systemd/system/siptools_research-3.timer

	# Use Python setuptools
	python3 ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true
	rm -rf coverage.xml htmlcov junit.xml .coverage

clean-rpm:
	rm -rf rpmbuild


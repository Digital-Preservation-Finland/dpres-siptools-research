DESTDIR ?=
PREFIX ?= /usr
ETC=${DESTDIR}/etc
SHAREDIR=${DESTDIR}${PREFIX}/share/siptools_research
VAR=${DESTDIR}/var

LOGDIR=${VAR}/log/siptools_research
PROCESSINGDIR=${VAR}/spool/siptools_research
CONFDIR=${ETC}/siptools_research

install:
	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Create log, share and processing directories
	mkdir -p "${LOGDIR}"
	mkdir -p "${PROCESSINGDIR}"
	mkdir -p "${SHAREDIR}"
	mkdir -p "${CONFDIR}"

	# Copy config file
	cp include/etc/siptools_research/conf ${CONFDIR}/

	# Use Python setuptools
	python setup.py build ; python ./setup.py install -O1 --prefix="${DESTDIR}${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES
	cat INSTALLED_FILES | sed 's/^/\//g' >> INSTALLED_FILES

test:
	# TODO: fix test_import_object.py and remove "ignore" switch
	py.test -v tests --ignore tests/utils/scripts/test_import_object.py

test-junitxml:
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

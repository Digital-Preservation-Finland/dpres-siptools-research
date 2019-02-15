DESTDIR ?= /
PREFIX ?= /usr
ETC=${DESTDIR}/etc
SHAREDIR=${DESTDIR}${PREFIX}/share/siptools_research
VAR=${DESTDIR}/var
LIB=${DESTDIR}${PREFIX}/lib

LOGDIR=${VAR}/log/siptools_research
PROCESSINGDIR=${VAR}/spool/siptools_research

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
	cp include/etc/dpres_mimetypes.json ${ETC}/
	cp include/usr/lib/systemd/system/siptools_research.service ${LIB}/systemd/system/siptools_research.service
	cp include/usr/lib/systemd/system/siptools_research.timer ${LIB}/systemd/system/siptools_research.timer

	# Use Python setuptools
	python ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES

	# Remove requires.txt from egg-info because it contains PEP 508 URL requirements
	# that break siptools-research on systems that use old version of
	# python setuptools (older than v.20.2)
	rm ${DESTDIR}${PREFIX}/lib/python2.7/site-packages/*.egg-info/requires.txt
	sed -i '/\.egg-info\/requires.txt$$/d' INSTALLED_FILES

test:
	py.test  tests -svvvv --junitprefix=dpres-siptools-research --junitxml=junit.xml \
		--ignore tests/integration_tests/ida_integration_test.py \
		--ignore tests/integration_tests/metax_integration_test.py \
		--ignore tests/integration_tests/report_preservation_status_test.py \
		--ignore tests/integration_tests/validate_sip_test.py \
		--ignore tests/integration_tests/send_sip_test.py

coverage:
	py.test tests --cov=siptools_research --cov-report=html --ignore tests/integration_tests/ --ignore tests/workflow/report_preservation_status_test.py
	coverage report -m
	coverage html
	coverage xml

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true
	rm -rf coverage.xml htmlcov junit.xml .coverage

clean-rpm:
	rm -rf rpmbuild
	rm -rf doc/build doc/modules

rpm: clean
	create-archive.sh
	preprocess-spec-m4-macros.sh include/rhel7
	build-rpm.sh

.PHONY: doc
doc:
	PYTHONPATH="../" make -C doc html

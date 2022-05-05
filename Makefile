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
	cp include/usr/lib/systemd/system/siptools_research-3.service ${LIB}/systemd/system/siptools_research-3.service
	cp include/usr/lib/systemd/system/siptools_research.timer ${LIB}/systemd/system/siptools_research-3.timer

	# Use Python setuptools
	python3 ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES

test:
	${PYTHON} -m pytest \
	    tests/unit_tests \
		tests/integration_tests/dependency_tree_test.py \
		tests/integration_tests/report_preservation_status_test.py \
		tests/integration_tests/send_sip_test.py \
		tests/integration_tests/validate_sip_test.py \
		tests/integration_tests/workflow_test.py -svvvv -o junit_family=xunit1 --junitprefix=dpres-siptools-research --junitxml=junit.xml

coverage:
	${PYTHON} -m pytest tests \
	    --cov=siptools_research --cov-report=html \
	    --ignore tests/integration_tests/ \
	    --ignore tests/workflow/report_preservation_status_test.py
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

.PHONY: doc
doc:
	PYTHONPATH="../" make -C doc html

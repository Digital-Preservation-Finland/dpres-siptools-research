ROOT=/
PREFIX=/usr
ETC=/etc
SHAREDIR=${PREFIX}/share/siptools_research
VAR=/var

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

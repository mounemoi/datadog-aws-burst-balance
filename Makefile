PYTHON_PATH=/opt/datadog-agent/embedded/bin/

.PHONY: default test coding-rule

default:
	# do nothing

test:
	# TODO
	# PYTHONPATH=checks.d/:tests/dummy/ \
	#    ${PYTHON_PATH}python -m unittest -v tests.test_hoge

coding-rule:
	find ./ -name "*.py" | ${PYTHON_PATH}flake8 --config ./.config/flake8


pip-install:
	pip install -r requirements.txt

pip-freeze:
	pip freeze > requirements.txt

pip-clean:
	pip uninstall -r requirements.txt -y
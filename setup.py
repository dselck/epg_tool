from setuptools import setup

install_requires = [
    'pandas',
    'fuzzywuzzy',
    'tmdbsimple @ git+https://github.com/dselck/tmdbsimple.git',
    'lxml',
    'python-Levenshtein',
    'schedule',
]

setup(
    name='epg_tool',
    version='0.0.1',
    description='A python project to combine EIT and EPG data including enriching',
    url='https://github.com/dselck/epg_tool',
    classifiers=[
        'Development Status :: 3 - Alpha'
    ],
    packages=['epg_tool'],
    python_requires='>3.4',
    install_requires=install_requires,
    scripts=['scripts/run_scheduled_xmltv_pulls.py', 'scripts/run_xmltv_pulls_once.py']
)
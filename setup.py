from setuptools import setup, find_packages
setup(
    name='transfercoder',
    version='3.2',
    description='A script to transfer and transcode your music library',
    url='https://github.com/DarwinAwardWinner/transfercoder',
    author='Ryan C. Thompson',
    author_email='rct+transfercoder@thompsonclan.org',
    license='GPLv2+',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: End Users/Desktop',
        'Environment :: Console',
        'Topic :: Multimedia :: Sound/Audio :: Conversion',
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='audio transcode',
    packages=find_packages(exclude=['scripts']),
    install_requires=[
        'ffmpy',
        'mutagen',
        'plac',
        'six',
    ],
    extras_require = {
        'progress_bars':  ["tqdm"],
    },
    scripts=['scripts/transfercoder',],
)

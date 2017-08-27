from setuptools import setup

setup(name='noaaport',
      version='1.2.1',
      description='NOAAPORT and EMWIN weather data feed library',
      url='https://github.com/JeremyGrosser/noaaport',
      license='BSD-2-Clause',
      author='Jeremy Grosser',
      author_email='jeremy@synack.me',
      packages=['noaaport'],
      entry_points={
          'console_scripts': [
              'noaaport-pipe=noaaport.pipe:main',
          ],
      })

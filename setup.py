from setuptools import setup

setup(name='noaaport',
      license='BSD-2-Clause',
      author='Jeremy Grosser',
      author_email='jeremy@synack.me',
      packages=['noaaport'],
      entry_points={
          'console_scripts': [
              'noaaport-pipe=noaaport.pipe:main',
          ],
      })

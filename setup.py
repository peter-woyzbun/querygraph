from setuptools import setup


setup(name='querygraph',
      version='0.1',
      description='Query Graph package.',
      url='http://github.com/peter-woyzbun/',
      author='Peter Woyzbun',
      author_email='peter.woyzbun@gmail.com',
      packages=['querygraph'],
      install_requires=['pandas', 'numpy', 'pyparsing', 'pyyaml'],
      zip_safe=False)
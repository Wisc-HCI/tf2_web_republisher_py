from setuptools import setup

package_name = 'tf2_web_republisher_py'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools','numpy','pyquaternion'],
    zip_safe=True,
    maintainer='schoen',
    maintainer_email='schoen.andrewj@gmail.com',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'tf2_web_republisher = tf2_web_republisher_py.tf2_web_republisher_py:main'
        ],
    },
)

import setuptools


setuptools.setup(
    name='hgame-ident',
    description='Person identification in statistical data',
    version='0.1',
    packages=['hgame.ident'],
    install_package_data=True,
    python_requires=">=3.7",
    install_requires=[
        'hgame-cmdline',
        'pandas', 'tabulate'
    ],
    entry_points="""
        [hgame.cli_plugins]
        ident=hgame.ident.main:ident
    """,
)

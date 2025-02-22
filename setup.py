from setuptools import setup, find_packages

setup(
    name="financial_risk_management",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pyspark==2.4.8',
        'pandas',
        'numpy',
        'scikit-learn',
        'python-dotenv',
        'joblib'
    ]
) 
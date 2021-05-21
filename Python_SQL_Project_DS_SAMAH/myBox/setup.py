#!/usr/bin/env python
# coding: utf-8

# In[2]:


import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="myBox-qutaishat",
    version="0.0.1",
    author="Samah qutaishat",
    author_email="samah-samir-a--alkayed.ktaishat@edu.dsti.institute",
    description="myBox offers utility classes and functions for dealing with the DSTI combined SQL & Python project",
    url="",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
)


# In[ ]:





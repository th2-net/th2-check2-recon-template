## Introduction
This component allows you to compare two message streams and publish the comparison result to the report. 
Comparison is made using rules, the interface of which is implemented by the user for various tasks.
The two main methods for comparing messages are `hash(message)` and `check(message_a, message_b)`.
Method `hash()` computes a hash string from the received message and returns it. 
Method `check()` makes a more detailed comparison of received messages and returns `Event` of comparison.

## Installing

Use the following command to install the required dependencies:
```
pip install -r requirements.txt
```

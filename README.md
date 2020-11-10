# th2-check2-recon
This is one of the parts of the `th2-check2-recon` component, or `recon` in short. 
`Recon` allows you to compare message streams with each other using specified scenarios called `Rule`. 
This part is called `th2-check2-recon-template`. 
It contains the implementation of the `rules` and the entry point. 
The main logic and everything you need to create a `rule` is contained in the `th2_check2_recon` library from `th2-pypi` Nexus repository.


To implement your `rule`, you need to implement the `Rule` class in a separate file in the directory specified
 in the `rules_package_path` parameter of the `custom` config, inheriting it from 
 the `th2_check2_recon.rule.Rule` class from the core library 
 (an example of implementation can be seen in the demo rules in src/rules).


To implement rule means override:
* `get_name()` - specify the name of `rule` in GUI.
* `get_description()`- specify the description of `rule` in GUI.
* `get_attributes()` - specify required message stream attributes. 
 Example, `[['parsed', 'subscribe'], ['parsed', 'fix']]`
* `description_of_groups()` - specify `message groups` into which messages will be distributed.
* `group(message, attributes)` - specify which group to put the incoming message.
* `hash(message, attributes)` - return computed message hash. This hash is used to quickly compare messages.
* `check(messages)` - compares messages in detail. Return `Event` for report in GUI.

The lifecycle of an incoming message is:
1. Comes in `rule` from some kind of `pin`. A record about this is written to `log`.
2. The `group(message, attributes)` method is called for this message. 
It is calculated in which `message group` the message should be placed.
3. The hash of the message is calculated using the `hash(message, attributes)`.
4. Searches for messages with the same hash in other `message groups`.
5. If a message with the same hash is found in each group, `check(messages)` is called for all these messages. 
Depending on the types of `message groups` and their number, 
it will be determined which messages to delete and which to keep.
6. If no similar messages are found, then just add the message to the group.

# Recon configuration

* `recon_name` - name report in GUI.
* `cache_size` - maximum `message group` size.
* `rules_package_path` - directory where `rules` are located.
* `event_batch_max_size` - maximum number of events in one `EventBatch`.
* `event_batch_send_interval` - how often to send `EventBatch` with events.
* `rules` - list of `rule` configurations

# Rule configuration

+ `name` - name of the file containing the rule.
+ `enabled` - should `rule` be used or not.
+ `match_timeout` - time interval between compared messages in seconds.
+ `match_timeout_offset_ns` - time interval between compared messages offset in nanoseconds.
+ `configuration` - customized additional settings for `rule`.

# Installing
To install all the necessary dependencies, you need to install all the packages from `requirements.txt`.
 This can be done with `pip install -r requirements.txt`.

      
    
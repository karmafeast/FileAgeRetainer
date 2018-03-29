# FileAgeRetainer

## you can generate fancy documentation using the awful sandcastle tool
## code's marked up so it'll work.

## SEE SUCH A HELP FILE IN THE ROOT OF REPO (from debug build, next to where you found this file)

The service will bring up a list of FileAgeRetainer items from the app.config

These will end up figuring out what index they are on on this list so they can access it directly.  

TLDR; skip this para: This is used in a recovery mechanism (not implemented in this version, which I used when I found on super slow disks the file system buffers could overflow, leaving that RetainerItem crashed.  basically, quiesce them all, snip the deaded one - detected by no activity on it, or stage changes in its processing, then bring it back up as a new item on the end of the list - can use the serialized dicts still if valid, and have everyone else recalculate their index.  it results in ressurection!)

each RetainerItem has a:

##QueueWatcher 
 - this runs the file system watchers and handles queues and the simplification of actions in fs happening as atomic stages - e.g. in a file move)

##FsMaintenanceWorker
 - does actual file system work.  read only flag set by some crazy app we don't know about and we're configed not to respect that, sets it off, handles the delete. etc.

##SerializationWorker
 - periodically serializes the FileAgeRetainerItem - the service will look for dictionaries by calculating a hash on the config line items in app.config RetainedFileRoots -- so if you change your rules, you invalidate the prior dictionary, but if you change them back and are within the tolerance range we'll bring back in what we can of it.

This was really fun to do for me - and the localization team LOVED it!  once we had proper disks in the aws system (which i could prove need for from queueing and raw iops testing synthetically local to vm instance) the entire business processes involved in media exchange internationally became a lot easier.  upload / download speeds improved too - while not a direct affect of my system handling file content expiry - but as a result of analyzing their problem and devloping a comprehensive and scalable solution for it.  

the FileAgeRetainer can go on fast disks just fine.  set the preservation time to less than a minute as fractions of 1 and you can accelerate its interactions.

Grabbing all resources was also not the goal.  In operation the FileAgeRetainer uses very little memory (the dictionary is bare minimum, open one up) and while of course increasing with disk activity uses very little cpu.


#!/bin/bash

rsync -azvv ags@c67ags1.jennings.home:/home/ags/arcgis/server/usr/directories/arcgissystem/arcgisinput /home/ags/arcgis/server/usr/directories/arcgissystem/ --delete --exclude-from=/home/ags/arcgis/sync/rsync_excludes.txt &> ~/arcgis/sync/rsync_results.log

python ~/arcgis/sync/syncServices.py &> ~/arcgis/sync/syncServices.log


rsync -azvv ags@c67ags1.jennings.home:/home/ags/arcgis/server/usr/config-store/services /home/ags/arcgis/server/usr/config-store/ --delete --exclude-from=/home/ags/arcgis/sync/rsync_excludes.txt &> ~/arcgis/sync/rsync_results2.log
~


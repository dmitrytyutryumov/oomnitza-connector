# The Oomnitza Connector
Oomnitza has created a unified connector, lovingly crafted using Python, which is a single application that
 can be used to pull data from multiple sources and push it to your Oomnitza application. The connector can
 presently pull data from the following sources, with more planned in the future.

* Airwatch [http://www.air-watch.com](http://www.air-watch.com/)
* BambhooHR [http://www.bamboohr.com](http://www.bamboohr.com/)
* Casper (Jamf Pro) [https://www.jamf.com/products/Jamf-Pro/](https://www.jamf.com/products/Jamf-Pro/)
* Jasper [http://www.jasper.com](http://www.jasper.com/)
* LDAP e.g., [http://www.openldap.org](http://www.openldap.org/), [Active Directory](https://www.microsoft.com)
* MobileIron [http://www.mobileiron.com](http://www.mobileiron.com/)
* Okta [https://www.okta.com](https://www.okta.com/)
* OneLogin [https://www.onelogin.com](https://www.onelogin.com/)
* SCCM [http://www.microsoft.com](http://www.microsoft.com/en-us/server-cloud/products/system-center-2012-r2-configuration-manager/)
* ZenDesk [https://www.zendesk.com](https://www.zendesk.com/)
* Apple DEP [http://www.apple.com/business/dep/](http://www.apple.com/business/dep/)
* Plain CSV files

The Oomnitza Connector can be hosted on Oomnitza's
 server cloud, free of charge, if the third party server is
 or can be made accessible from the Oomnitza Cloud. Contact us for more details!
 Organizations with dedicated internal services may prefer to run this connector
 in-house, behind the same firewall that prevents outside access.
## Getting Started
The most current version of this documentation can always be found on
 [GitHub](https://github.com/Oomnitza/oomnitza-connector/blob/master/README.md).

Since Oomnitza is highly customizable, there are many possibilities with the connector. Because of this, it is
 important to think ahead about what data you want to bring in and how you want to store it. Before we
 begin, take time to think about what information you want, and what Oomnitza fields you want filled out with Casper
 data. If the fields you want to map in haven’t been created yet, now is a good time to do so.
 (Refer to our [Guide to creating custom fields in Oomnitza](https://wiki.oomnitza.com/wiki/Creating_Fields_in_Oomnitza)
 to get started.)

## Getting the Connector
The Oomnitza Connector code is hosted at
[https://github.com/Oomnitza/oomnitza-connector](https://github.com/Oomnitza/oomnitza-connector).

The Oomnitza Connector can also be downloaded from within your Oomnitza instance. Log into your instance and
 navigate to the System Settings page. Scroll to the bottom of the Integrations page and download either the
 correct binary or the “Source Code” Package.
* If you will be hosting the connector on a Windows or Mac server, we recommend using the binary version.
* The Source Code package can be use on a Linux server, as well as Windows and Mac. This package requires
  that a python environment be setup properly, which the binary version avoids.


## Runtime Environment Setup
If you choose to run the binary version of the connector, you can skip this section. If you choose
 to install and run the python code, you will need to install Python 2.7.X as well as the packages which the connector
 relies upon. Some of the python packages may require build tools to be installed.

Please visit the sections below related to the build tools before installing the additional modules.

We suggest you setup a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
 and use `pip` to install the requirements. This can be done as follows (See our
 [documentation](https://wiki.oomnitza.com/wiki/Installing_additional_Python_Modules) on installing
 additional Python modules for use in Oomnitza.):

    cd /path/to/connector
    virtualenv .
    source bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    
Also if you need the SCCM functionality you have to separately install the `pyodbc` package 
(by default it is commented in the requirements.txt)
    
    pip install pyodbc    

### Linux Environment
On Ubuntu, the build tools are installed using:

    sudo apt-get install build-essential

### Windows Environment
For MS Windows you have to install Windows C++ compilers as build tools. Please visit the [Python Wiki](https://wiki.python.org/moin/WindowsCompilers)
 to check what is appropriate compiler you have to download and install.
 
### OS X Environment
For OS X environment you have to install the build tools using the following command:

    xcode-select --install
    
### Connector GUI
Optional feature. GUI provides the convenient interface to the config editing. 
 To support the GUI functionality you have to install wxPython package.
 
    pip install wxPython

### Packaging the connector
Sometimes you need the connector not as plain python script but as standalone executable. 
 In the repository there are 2 specification files for the [PyInstaller](http://www.pyinstaller.org/) tool. 
 You can easily package the connector into single executable for MS Windows and OS X. 
 First install the tool:
    
    pip install pyinstaller
    
 Next package the connector for target OS:
 
    pyinstaller pyinstaller.win.spec
   
 or
 
    pyinstaller pyinstaller.mac.spec

## Running the connector client
The connector is meant to be run from the command line and as such as multiple command line options:

    $ python connector.py -h
    usage: connector.py [-h] [--record-count RECORD_COUNT] [--singleton SINGLETON]
                        [--version] [--workers WORKERS] [--show-mappings]
                        [--testmode] [--save-data] [--ini INI]
                        [--logging-config LOGGING_CONFIG]
                        {upload,generate-ini,gui} [connectors [connectors ...]]
    
    positional arguments:
      {upload,generate-ini,gui}
                            Action to perform.
      connectors            Connectors to run.
    
    optional arguments:
      -h, --help            show this help message and exit
      --record-count RECORD_COUNT
                            Number of records to pull and process from connection.
      --singleton SINGLETON
                            Control the behavior of connector. Limiting the number
                            of simultaneously running connectors
      --version             Show the connector version.
      --workers WORKERS     Number of async IO workers used to pull & push
                            records.
      --show-mappings       Show the mappings which would be used by the
                            connector.
      --testmode            Run connectors in test mode.
      --save-data           Saves the data loaded from other system.
      --ini INI             Config file to use.
      --logging-config LOGGING_CONFIG
                            Use to override logging config file to use.

The available actions are:

* `gui` (default if wxPython is installed): launch the config gui. Not accessible if wxPython is not installed.
* `generate-ini`: generate an example config.ini file.
* `upload`: uploads the data from the indicated connectors to Oomnitza. The connector values are taken
   from the section names in the ini file.

`--ini` is used to specify which config file to load, if not provided, `config.ini` from the root directory will be used.
   This option can be used with the `generate-ini` action to specify the file to generate.

`--logging-config` is used to specify an alternate logging config file.

`--show-mappings` is used to print out the loaded mappings. These mappings can be a combination of the built-in mappings,
   config.ini mappings, and mappings setup via the website.

`--testmode` will print out the records which would have been sent rather than pushing the data to the server. This
   can be used to see what, exactly, is getting sent to the server.

`--record-count` is used to limit the number of records to process. Once this number have been processed, the connector
   will exit. This can be used with `--testmode` to print out a limited number of records then exit cleanly.

`--save-data` is used to save the data loaded from the remote system to disk. These files can then be used to confirm
   the data is being loaded and mapped as expected.
   
`--singleton` is used to switch off the default connector executable behaviour preventing to run multiple executables at once. 
   Set it as 0 to disable this restriction and enable multiple running executables 

    python connector upload ldap --singleton=0

`--workers` is used to setup the number of workers used to push the extracted data to Oomnitza instance. Default is 2. 
   If you will increase this value it will increase the load generated by connector and decrease the time required to finish the full sync.


## Setting the connector to run as an automated task
There are many ways to automate the sync, here are a few:

* OS X: http://www.maclife.com/article/columns/terminal_101_creating_cron_jobs
* OS X: http://superuser.com/questions/126907/how-can-i-get-a-script-to-run-every-day-on-mac-os-x
* OS X: http://launched.zerowidth.com/
* Linux: http://www.cyberciti.biz/faq/how-do-i-add-jobs-to-cron-under-linux-or-unix-oses/
* Windows: http://bytes.com/topic/python/answers/32605-windows-xp-cron-scheduler-python


## Running the connector server
It is possible to setup the connector server that will handle webhooks and other requests from external sources and react to them. 
The connector server is WSGI compliant server (https://en.wikipedia.org/wiki/Web_Server_Gateway_Interface).
The connector server is meant to be run from the command line with following command line arguments:

    $ python server.py -h
    usage: server.py [-h] [--host HOST] [--port PORT] [--version]
                     [--show-mappings] [--testmode] [--save-data] [--ini INI]
                     [--logging-config LOGGING_CONFIG]
    
    optional arguments:
      -h, --help            show this help message and exit
      --host HOST
      --port PORT
      --version             Show the connector version.
      --show-mappings       Show the mappings which would be used by the
                            connector.
      --testmode            Run connectors in test mode.
      --save-data           Saves the data loaded from other system.
      --ini INI             Config file to use.
      --logging-config LOGGING_CONFIG
                            Use to override logging config file to use.

The available arguments for the connector server are serving for the same purposes as for the connector client, except 2 new server-specific arguments:

`--host` is used to specify the server's host. Default is 127.0.0.1

`--port` is used to specify the server's port. Default is 8000

**Note:** Now only the Casper (JAMF Pro) Webhooks are supported out of the box by the connector server.
First you have to enable webhooks with JSON payloads (http://docs.jamf.com/9.96/casper-suite/administrator-guide/Webhooks.html)
Out of the box the following webhooks are supported:
 
* ComputerAdded
* ComputerCheckIn
* ComputerInventoryCompleted
* ComputerPolicyFinished
* ComputerPushCapabilityChanged
* MobileDeviceCheckIn
* MobileDeviceCommandCompleted
* MobileDeviceEnrolled
* MobileDevicePushSent
* MobileDeviceUnEnrolled

The url pointing to the connector server instance should ends with the name of the connector:

Examples:

    https://my-connector-server.com/it/does/not/matter/casper
    https://my-connector-server.com/it/does/not/matter/casper.MDM
    https://my-connector-server.com/it/does/not/matter/casper.1

## Connector Configs

Now you should be able to generate a default config file. Running `python connector.py generate-ini` will regenerate
 the config.ini file, and create a backup if the file already exists. When you edit this file, it will have one section
 per connection. You can safely remove the section for the connections you will not be using to keep the file small and
 manageable.

If you require multiple different configurations of a single connector, such as the need to pull from two different LDAP OUs,
 additional sections can be added by appending a '.' and a unique identifier to the section name. For example, having both a
 `[ldap]` and `[ldap.Contractors]` section will allow you to pull users from a default and Contractor OU.

An example generated config.ini follows.

    [oomnitza]
    url = https://example.oomnitza.com
    api_token =
    username = oomnitza-sa
    password = ThePassword

    [airwatch]
    enable = False
    url = https://apidev.awmdm.com
    username = username@example.com
    password = change-me
    api_token = YOUR AirWatch API TOKEN
    sync_field = 24DCF85294E411E38A52066B556BA4EE

    [appledep]
    enable = False
    url = https://mdmenrollment.apple.com
    api_token = YOUR APPLE DEP SERVER TOKEN
    sync_field = 24DCF85294E411E38A52066B556BA4EE

    [bamboohr]
    enable = False
    url = https://api.bamboohr.com/api/gateway.php
    system_name = YOUR BambooHR SYSTEM NAME
    api_token = YOUR BambooHR API TOKEN
    default_role = 25

    [casper]
    enable = False
    url = https://jss.jamfcloud.com/example
    username = username@example.com
    password = change-me
    sync_field = 24DCF85294E411E38A52066B556BA4EE
    sync_type = computers
    update_only = False

    [jasper]
    enable = False
    wsdl_path = http://api.jasperwireless.com/ws/schema/Terminal.wsdl
    username = username@example.com
    password = change-me
    storage = storage.db
    api_token = YOUR Jasper API TOKEN
    sync_field = 24DCF85294E411E38A52066B556BA4EE
    update_only = False

    [ldap]
    enable = False
    url = ldap://ldap.forumsys.com:389
    username = cn=read-only-admin,dc=example,dc=com
    password =
    base_dn = dc=example,dc=com
    protocol_version = 3
    filter = (objectClass=*)
    default_role = 25
    default_position = Employee

    [mobileiron]
    enable = False
    url = https://na1.mobileiron.com
    username = username@example.com
    password = change-me
    partitions = ["Drivers"]
    sync_field = 24DCF85294E411E38A52066B556BA4EE

    [okta]
    enable = False
    url = https://example-admin.okta.com
    api_token = YOUR Okta API TOKEN
    default_role = 25
    default_position = Employee
    deprovisioned = false

    [onelogin]
    enable = False
    url = https://app.onelogin.com/api/v2/users.xml
    api_token = YOUR OneLogin API TOKEN
    default_role = 25
    default_position = Employee

    [sccm]
    enable = False
    server = server.example.com
    database = CM_DCT
    username = change-me
    password = change-me
    authentication = SQL Server
    sync_field = 24DCF85294E411E38A52066B556BA4EE

    [zendesk]
    enable = False
    system_name = oomnitza
    api_token = YOUR Zendesk API TOKEN
    username = username@example.com
    default_role = 25
    default_position = Employee
    
    [csv_assets]
    enable = False
    filename = /some/path/to/file/assets.csv
    directory = /some/path/to/files
    sync_field = BARCODE

    [csv_users]
    enable = False
    filename = /some/path/to/file/users.csv
    directory = /some/path/to/files
    default_role = 25
    default_position = Employee


The `[oomnitza]` section is where you configure the connector with the URL and login credentials for connecting to
Oomnitza. You can use an existing user’s credentials for username and password, but best practice is to create a
service account using your standard naming convention. (See the (documentation)[http://docs)
for managing user accounts in Oomnitza.)

The remaining sections each deal with a single connection to an external service. The "enable" field is common to all
connections and if set to "True" will enable this service for processing. Some fields are common to a type of
connection. For example, "default_role" and "default_user" are fields for connections dealing with loading
People into the Oomnitza app.

Each section can end with a list of field mappings. Simple mappings which just copy a field from the external system to
 a field inside Oomnitza can be defined here or in the System Settings within Oomnitza. Simple mappings are as follows:

    mapping.[Oomnitza Field] = {"source": "[external field]"}

For fields which require processing before being brought into Oomnitza must be defined in the INI. These mappings are
 more involved. Please contact [support@oomnitza.com](mailto://support@oomnitza.com) for more information. The format is:

    mapping.[Oomnitza Field] = {"source": "[external field]", "converter": "[converter name]"}

### Oomnitza Configuration
`url`: the url of the Oomnitza application. For example: `https://example.oomnitza.com`

`username`: the Oomnitza username to use

`password`: the Oomnitza password to use

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`api_token`: The API Token belonging to the Oomnitza user. If provided, `password` must be left blank.

### Airwatch Configuration
`url`: the url of the Airwatch server

`username`: the Airwatch username to use

`password`: the Airwatch password to use

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`api_token`: API token for the connection

`sync_field`:  The Oomnitza field which contains the asset's unique identifier (we typically recommend serial number).

#### Default Field Mappings
    To Be Determined


### Apple DEP Configuration
`url`: the url of the Apple DEP MDM server

`api_token`: the server token that should be obtained from Apple DEP MDM server

`sync_field`:  The Oomnitza field which contains the asset's unique identifier (we typically recommend serial number).

#### Default Field Mappings
    To Be Determined


### CSV Assets Configuration
`filename`: CSV file with assets inside

`directory`: directory with CSV files with assets inside. Note: `filename` and `directory` are mutually exclusive

`sync_field`:  The Oomnitza field which contains the asset's unique identifier (we typically recommend serial number).

#### Default Field Mappings
    No default mapping. Everything should be defined in the config


### CSV Users Configuration
`filename`: CSV file with assets inside

`directory`: directory with CSV files with assets inside. Note: `filename` and `directory` are mutually exclusive

`default_role`: The numeric ID of the role which will be assigned to imported users. For example: `25`.

`default_position`: The position which will be assigned to the user. For example: `Employee`.

#### Default Field Mappings
    No default mapping. Everything should be defined in the config


### BambooHR Configuration
`url`: the url of the BambooHR server

`system_name`: Identifier of your system in the Bamboo HR environment

`api_token`: API token for the connection

`default_role`: The numeric ID of the role which will be assigned to imported users. For example: `25`.

`default_position`: The position which will be assigned to the user. For example: `Employee`.

#### Default Field Mappings
    mapping.USER =            {'source': "workEmail"}
    mapping.FIRST_NAME' =     {'source': "firstName"}
    mapping.LAST_NAME' =      {'source': "lastName"}
    mapping.EMAIL' =          {'source': "workEmail"}
    mapping.PHONE' =          {'source': "mobilePhone"}
    mapping.POSITION' =       {'source': "jobTitle"}
    mapping.PERMISSIONS_ID' = {'setting': "default_role"}


### Casper Configuration
The `[casper]` section contains a similar set of preferences; your JSS URL, and the login credentials for an auditor
account in Casper (See the [Casper Suite Administrator’s Guide](http://resources.jamfsoftware.com/documents/products/documentation/Casper-Suite-9.63-Administrators-Guide.pdf?mtime=1420481585), pg. 42).

The identifier section of the config.ini file should contain a mapping to a unique field in Oomnitza, which you want to
use as the identifier for an asset. Serial Number is the most commonly used identifier since no two assets should share
one. This will determine if the script creates a new record for a given serial number on its next sync, or if it updates
an existing record that has new information.

`url`: the url of the Casper server

`username`: the Casper username to use

`password`: the Casper password to use. Note: the Casper API will **_NOT_** work with a password which contains `%` or `*`. `!` is an acceptable character to use.

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`sync_field`: The Oomnitza field which contains the asset's unique identifier (we typically recommend serial number).

`sync_type`: Sets the type of data to pull from Casper. Options are `computers` or `mobiledevices`. 
**Note**: If you need to pull computers AND mobile devices info from Casper, copy Casper configuration section to the same config.ini and name it as 'casper.MDM'. 
Set the field mapping related to computers in the 'casper' section and set **sync_type = computers**. Set the field mapping related to mobile devices in the 'casper.MDM' section and set **sync_type = mobiledevices**

`group_name`: Specifies the Group from which to load assets. If `group_name` is missing or empty, all assets will be loaded.
  If present, only assets from this Group will be processed.
  
`verify_ssl`: set to false if the Casper server is running with a self signed SSL certificate.

`update_only`: set this to True to only update records in Oomnitza. Records for new assets will not be created.

#### List of currently supported Casper external fields (computers)
    'general.alt_mac_address'
    'general.asset_tag'
    'general.barcode_1'
    'general.barcode_2'
    'general.distribution_point'
    'general.id'
    'general.initial_entry_date'
    'general.initial_entry_date_epoch'
    'general.initial_entry_date_utc'
    'general.ip_address'
    'general.jamf_version'
    'general.last_cloud_backup_date_epoch'
    'general.last_cloud_backup_date_utc'
    'general.last_contact_time'
    'general.last_contact_time_epoch'
    'general.last_contact_time_utc'
    'general.mac_address'
    'general.mdm_capable'
    'general.name'
    'general.netboot_server'
    'general.platform'
    'general.report_date'
    'general.report_date_epoch'
    'general.report_date_utc'
    'general.serial_number'
    'general.sus'
    'general.udid'
    'hardware.active_directory_status'
    'hardware.available_ram_slots'
    'hardware.battery_capacity'
    'hardware.boot_rom'
    'hardware.bus_speed'
    'hardware.bus_speed_mhz'
    'hardware.cache_size'
    'hardware.cache_size_kb'
    'hardware.make'
    'hardware.model'
    'hardware.model_identifier'
    'hardware.nic_speed'
    'hardware.number_processors'
    'hardware.optical_drive'
    'hardware.os_build'
    'hardware.os_name'
    'hardware.os_version'
    'hardware.processor_architecture'
    'hardware.processor_speed'
    'hardware.processor_speed_mhz'
    'hardware.processor_type'
    'hardware.service_pack'
    'hardware.smc_version'
    'hardware.total_ram'
    'hardware.total_ram_mb'
    'location.building'
    'location.department'
    'location.email_address'
    'location.phone'
    'location.position'
    'location.real_name'
    'location.room'
    'location.username'
    'purchasing.applecare_id'
    'purchasing.is_leased'
    'purchasing.is_purchased'
    'purchasing.lease_expires'
    'purchasing.lease_expires_epoch'
    'purchasing.lease_expires_utc'
    'purchasing.life_expectancy'
    'purchasing.os_applecare_id'
    'purchasing.os_maintence_expires'
    'purchasing.po_date'
    'purchasing.po_date_epoch'
    'purchasing.po_date_utc'
    'purchasing.po_number'
    'purchasing.purchase_price'
    'purchasing.purchasing_account'
    'purchasing.purchasing_contact'
    'purchasing.vendor'
    'purchasing.warranty_expires'
    'purchasing.warranty_expires_epoch'
    'purchasing.warranty_expires_utc'

#### List of currently supported Casper external fields (mobile devices)
    'general.airplay_password'
    'general.asset_tag'
    'general.available'
    'general.available_mb'
    'general.battery_level'
    'general.bluetooth_mac_address'
    'general.capacity'
    'general.capacity_mb'
    'general.bluetooth_mac_address'
    'general.cloud_backup_enabled'
    'general.device_id'
    'general.device_name'
    'general.device_ownership_level'
    'general.display_name'
    'general.do_not_disturb_enabled'
    'general.id'
    'general.initial_entry_date_epoch'
    'general.initial_entry_date_utc'
    'general.ip_address'
    'general.itunes_store_account_is_active'
    'general.last_backup_time_epoch'
    'general.last_backup_time_utc'
    'general.last_cloud_backup_date_epoch'
    'general.last_cloud_backup_date_utc'
    'general.last_inventory_update'
    'general.last_inventory_update_epoch'
    'general.last_inventory_update_utc'
    'general.locales'
    'general.managed'
    'general.model'
    'general.model_display'
    'general.model_identifier'
    'general.modelDisplay'  # looks like the same as 'general.model_display'
    'general.modem_firmware'
    'general.name'
    'general.os_build'
    'general.os_type'
    'general.os_version'
    'general.percentage_used'
    'general.phone_number'
    'general.serial_number'
    'general.supervised'
    'general.tethered'
    'general.udid'
    'general.wifi_mac_address'
    'location.building'
    'location.department'
    'location.email_address'
    'location.phone'
    'location.position'
    'location.real_name'
    'location.room'
    'location.username'
    'network.carrier_settings_version'
    'network.cellular_technology'
    'network.current_carrier_network'
    'network.current_mobile_country_code'
    'network.current_mobile_network_code'
    'network.data_roaming_enabled'
    'network.home_carrier_network'
    'network.home_mobile_country_code'
    'network.home_mobile_network_code'
    'network.iccid'
    'network.imei'
    'network.roaming'
    'network.voice_roaming_enabled'
    'purchasing.applecare_id'
    'purchasing.is_leased'
    'purchasing.is_purchased'
    'purchasing.lease_expires'
    'purchasing.lease_expires_epoch'
    'purchasing.lease_expires_utc'
    'purchasing.life_expectancy'
    'purchasing.po_date'
    'purchasing.po_date_epoch'
    'purchasing.po_date_utc'
    'purchasing.po_number'
    'purchasing.purchase_price'
    'purchasing.purchasing_account'
    'purchasing.purchasing_contact'
    'purchasing.vendor'
    'purchasing.warranty_expires'
    'purchasing.warranty_expires_epoch'
    'purchasing.warranty_expires_utc'
    'security.block_level_encryption_capable'
    'security.data_protection'
    'security.file_level_encryption_capable'
    'security.passcode_compliant'
    'security.passcode_compliant_with_profile'
    'security.passcode_present'
    
#### Default Field Mappings
    To Be Determined


### Jasper Configuration
`wsdl_path`: The full URL to the Terminal.wsdl. Defaults to: http://api.jasperwireless.com/ws/schema/Terminal.wsdl.

`username`: the Jasper username to use

`password`: the Jasper password to use

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`storage`: The path to the storage file used to maintain state about the connector. Defaults to: `storage.db`

`api_token`: The Jasper API Token.

`sync_field`: The Oomnitza field which contains the asset's unique identifier.

`update_only`: set this to True to only update records in Oomnitza. Records for new assets will not be created.

#### Default Field Mappings
    To Be Determined


### LDAP Configuration

`url`: The full URI for the LDAP server. For example: `ldap://ldap.forumsys.com:389`

`username`: the LDAP username to use. Can be a DN, such as `cn=read-only-admin,dc=example,dc=com`.

`password`: the LDAP password to use

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`base_dn`: The Base DN to use for the connection.

`protocol_version`: The LDAP Protocol version to use. Defaults to: 3.

`filter`: The LDAP filter to use when querying for people. For example: `(objectClass=*)` will load all objects under the base_db. This is a very reasonable default.

`default_role`: The numeric ID of the role which will be assigned to imported users. For example: `25`.

`default_position`: The position which will be assigned to the user. For example: `Employee`.


### MobileIron Configuration
`url`: The full URI for the MobileIron server. For example: `https://na1.mobileiron.com`

`username`: the MobileIron username to use.

`password`: the MobileIron password to use.

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`partitions`: The MobileIron partitions to load. For example: `["Drivers"]` or `["PartOne", "PartTwo"]`

`sync_field`: The Oomnitza field which contains the asset's unique identifier.

#### Default Field Mappings
    To Be Determined


### Okta Configuration
`url`: The full URI for the Okta server. For example: `https://oomnitza-admin.okta.com`

`api_token`: The Okta API Token.

`default_role`: The numeric ID of the role which will be assigned to imported users. For example: `25`.

`default_position`: The position which will be assigned to the user. For example: `Employee`.

`deprovisioned`: When it is `false` (default) the users with status `DEPROVISIONED` in Okta will not be pushed to Oomnitza.

#### Default Field Mappings
    mapping.USER =           {'source': "profile.login"},
    mapping.FIRST_NAME =     {'source': "profile.firstName"},
    mapping.LAST_NAME =      {'source': "profile.lastName"},
    mapping.EMAIL =          {'source': "profile.email"},
    mapping.PHONE =          {'source': "profile.mobilePhone"},
    mapping.PERMISSIONS_ID = {'setting': "default_role"},
    mapping.POSITION =       {'setting': "default_position"},


### OneLogin Configuration
`url`: The full URI for the OneLogin server. For example: `https://api.us.onelogin.com/api/1/users`

`client_id`: The Client ID used to connect to the API.
 
`client_secret`: The Client Secret used to connect to th API.

`api_token`: The OneLogin API Token. 
**Note:** OUTDATED. Is used for the old and outdated version of OneLogin API and left for compatibility reasons. 
if this old API is used another url should be set in the configuration as `url`: `https://app.onelogin.com/api/v2/users.xml`. 
If you have an issues during the connection to the OneLogin, please switch to the new API by defining the correct `client_id` and `client_secret` instead of `api_token`.

`default_role`: The numeric ID of the role which will be assigned to imported users. For example: `25`.

`default_position`: The position which will be assigned to the user. For example: `Employee`.

#### Defualt Field Mappings
    mapping.USER =           {'source': "username"}
    mapping.FIRST_NAME =     {'source': "firstname"}
    mapping.LAST_NAME =      {'source': "lastname"}
    mapping.EMAIL =          {'source': "email"}
    mapping.PHONE =          {'source': "phone"}
    mapping.PERMISSIONS_ID = {'setting': "default_role"}
    mapping.POSITION =       {'setting': "default_position"}


### SCCM Configuration
The account used to connect to the SCCM database requires at least read-only access.
**Note:** The SCCM connector currently requires a Windows host. While it should be possible to run the
connector on a non-Windows host, such as Linux, we do not provide support for this configuration at this time.

`server`: The server hosting the SCCM database.

`database`: The SCCM database from which to pull data.

`username`: The username to use when connecting to the server using `SQL Server` authentication. This user
requires read-only access to the DB. Ignored when using `Windows` authentication.

`password`: The password to use when connecting to the server using `SQL Server` authentication.
Ignored when using `Windows` authentication.

`env_password`: (optional) the name of the environment variable containing the password value to use. The `password` field will be ignored.

`authentication`: Sets the type of authentication to use when connecting to the server.
Options are `SQL Server` or `Windows`. The default is to use SQL Server Authentication.
When using `Windows` authentication, the `username` and `password` fields are ignored and the credentials
for the currently logged in user will be used when making the connection to the SCCM database.

`sync_field`: The Oomnitza field which contains the asset's unique identifier (we typically recommend serial number).

#### Default Field Mappings
    To Be Determined


### Zendesk Configuration
`system_name`: The Zendesk system name to use. For example: `oomnitza`

`api_token`: The Zendesk API Token.

`username`: the Zendesk username to use.

`default_role`: The numeric ID of the role which will be assigned to imported users. For example: `25`.

`default_position`: The position which will be assigned to the user. For example: `Employee`.

#### Default Field Mappings
    mapping.USER =           {'source': "email"}
    mapping.FIRST_NAME =     {'source': "name", 'converter': "first_from_full"}
    mapping.LAST_NAME =      {'source': "name", 'converter': "last_from_full"}
    mapping.EMAIL =          {'source': "email"}
    mapping.PHONE =          {'source': "phone"}
    mapping.PERMISSIONS_ID = {'setting': "default_role"}
    mapping.POSITION =       {'setting': "default_position"}


## Advanced usage

### Logging
The Oomnitza Connector uses the standard python `logging` module. This modules is configured via the `logging.json` file.
 This file can be edited, or copied to a new file, to change the logging behavior of the connector. Please see the
 [python docs](https://docs.python.org/2/library/logging.html) for information of configuring python logging.

### SSL Protocol Version
If the service to be connected to requires a particular SSL protocol version to properly connect, the connection's
 section in the ini file can include a `ssl_protocol` option. The value can be one of:
 `ssl`, `sslv23`, `sslv3`, `tls`, `tls1`.

### Custom Converters
It is possible to create a completely custom complex converter that will be used to convert values extracted from external system to before pushing them to the Oomnitza.
 To use this option you have to define the name of this converter in the mapping, like this
    
    mapping.MY_AWESOME_FIELD =  {"source": "name", "converter": "my_custom_converter"}
 
 next you have to define new `[converters]` section in the config with the `my_custom_converter:`. Under this converter name you have to define a valid Python 2.X function,
 that has to return some value - this value is a result of the converter. In the converter function a "record" object is available, it is the whole record extracted from external system as Python [dict](https://docs.python.org/2/library/stdtypes.html#dict) object.
 Example:

    [ldap]
    
    ... here goes config ... 
    
    mapping.POSITION = {"source": "position", "converter": "my_custom_converter"}
    
    [converters]
    my_custom_converter:
        return record.get("position", "Unknown position")
 If an exception is raised inside the custom converter's code, a None value is returned as the result

### Record Filtering
Support has been added for filtering the records passed from the connector to Oomnitza. By default, all records from the
 remote system will be sent to Oomnitza for processing. To limit the records based on values in those records, a special
 `recordfilter` value can be added to a connector section in the ini file. This filter is written using the Python
 programming language.

For example, the following filter will only  process records with the `asset_type` field set to "`computer`":

    recordfilter:
        return record.asset_type == "computer"

This is a very new feature, with many options, and we are still working on the documentation. If you are interested in
 using this feature, please contact [support@oomnitza.com](mailto://support@oomnitza.com) for assistance.

# The GUI

If you have installed [wxPython](https://wxpython.org/) in your system you will have an additional command line argument `gui` for the connector client.
 
    python connector.py gui

This will run the connector with graphical interface. This interface is used to configure the config.ini file. 
Unfortunately now this interface does not support all the sections of the config.ini, for example you cannot edit the custom converters or filters.

# Current limitations

###### Software mapping
There is no possibility to set the mapping for the software info associated with IT assets (SCCM, JAMF). The only thing can be done is to disable the mapping at all. 
To do this set the following custom mapping in your config.ini file:

    mapping.APPLICATIONS = {"hardcoded": []}

###### MS Windows environment
In MS Windows environment the "Task Scheduler" is the natural tool to schedule the connector execution. Please note that when the task is scheduled via "Task Scheduler" the "working directory" is not 
the directory of connector executable, but other one. So if you are using this tool to schedule the connector job, please also set the path to the configuration files via the command line arguments. 
Or schedule not the connector itself, but the .bat file which is triggering the connector  

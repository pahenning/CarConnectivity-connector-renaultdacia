
# CarConnectivity Connector for Renault and Dacia Vehicles
[![GitHub sourcecode](https://img.shields.io/badge/Source-GitHub-green)](https://github.com/tillsteinbach/CarConnectivity-connector-renaultdacia/)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/tillsteinbach/CarConnectivity-connector-renaultdacia)](https://github.com/tillsteinbach/CarConnectivity-connector-renaultdacia/releases/latest)
[![GitHub](https://img.shields.io/github/license/tillsteinbach/CarConnectivity-connector-renaultdacia)](https://github.com/tillsteinbach/CarConnectivity-connector-renaultdacia/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/tillsteinbach/CarConnectivity-connector-renaultdacia)](https://github.com/tillsteinbach/CarConnectivity-connector-renaultdacia/issues)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/carconnectivity-connector-renaultdacia?label=PyPI%20Downloads)](https://pypi.org/project/carconnectivity-connector-renaultdacia/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/carconnectivity-connector-renaultdacia)](https://pypi.org/project/carconnectivity-connector-renaultdacia/)
[![Donate at PayPal](https://img.shields.io/badge/Donate-PayPal-2997d8)](https://www.paypal.com/donate?hosted_button_id=2BVFF5GJ9SXAJ)
[![Sponsor at Github](https://img.shields.io/badge/Sponsor-GitHub-28a745)](https://github.com/sponsors/tillsteinbach)

## This connector is tested against a Dacia Spring. 
Executable commands include starting and stopping the air condition HVAC, refreshing the GPS location and getting the charge history of the previous 30 days

[CarConnectivity](https://github.com/tillsteinbach/CarConnectivity) is a python API to connect to various car services. This connector enables the integration of Renault and Dacia vehicles through the Renault/Kamereon API. Look at [CarConnectivity](https://github.com/tillsteinbach/CarConnectivity) for other supported brands.

## Configuration
In your carconnectivity.json configuration add a section for the renaultdacia connector like this:
```json
{
    "carConnectivity": {
        "connectors": [
            {
                "type": "renaultdacia",
                "config": {
                    "username": "test@test.de",
                    "password": "testpassword123",
                    "locale": "de_DE"
                }
            }
        ]
    }
}
```

The `locale` field controls which regional API keys and Gigya endpoint to use (default: `de_DE`).
See the list of supported locales below.

### Credentials
If you do not want to provide your username or password inside the configuration you can create a `.netrc` file at the appropriate location (usually your home folder):
```
# For Renault/Dacia
machine renaultdacia
login test@test.de
password testpassword123
```
In this case the configuration needs to look like this:
```json
{
    "carConnectivity": {
        "connectors": [
            {
                "type": "renaultdacia",
                "config": {
                    "locale": "de_DE"
                }
            }
        ]
    }
}
```

You can also provide the location of the netrc file in the configuration:
```json
{
    "carConnectivity": {
        "connectors": [
            {
                "type": "renaultdacia",
                "config": {
                    "netrc": "/some/path/on/your/filesystem",
                    "locale": "de_DE"
                }
            }
        ]
    }
}
```

### Supported Locales
The following locales are supported: `bg_BG`, `cs_CZ`, `da_DK`, `de_DE`, `de_AT`, `de_CH`, `en_GB`, `en_IE`, `es_ES`, `fi_FI`, `fr_FR`, `fr_BE`, `fr_CH`, `fr_LU`, `hr_HR`, `hu_HU`, `it_IT`, `it_CH`, `nl_NL`, `nl_BE`, `no_NO`, `pl_PL`, `pt_PT`, `ro_RO`, `ru_RU`, `sk_SK`, `sl_SI`, `sv_SE`.

### Known issues
#### Unexpected keys found
Not all items that are presented in the data from the server are already implemented by the connector. Feel free to report interesting findings in your log data in the [Discussions](https://github.com/tillsteinbach/CarConnectivity-connector-renaultdacia/discussions) section or as an [Issue (Enhancement)](https://github.com/tillsteinbach/CarConnectivity-connector-renaultdacia/issues).

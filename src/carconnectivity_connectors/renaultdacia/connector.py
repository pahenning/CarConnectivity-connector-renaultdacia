"""Module implements the connector to interact with the Renault/Dacia API."""
from __future__ import annotations
from typing import TYPE_CHECKING

import threading
import json
import os
import traceback
import logging
import netrc
from datetime import datetime, timezone, timedelta

import requests

from carconnectivity.garage import Garage
from carconnectivity.errors import AuthenticationError, TooManyRequestsError, RetrievalError, \
    TemporaryAuthenticationError
from carconnectivity.util import robust_time_parse, log_extra_keys, config_remove_credentials
from carconnectivity.units import Length, Volume
from carconnectivity.drive import ElectricDrive, CombustionDrive
# pah from carconnectivity.attributes import DurationAttribute, EnumAttribute
from carconnectivity.attributes import DurationAttribute, EnumAttribute, StringAttribute
from carconnectivity.units import Temperature
from carconnectivity.charging import Charging
from carconnectivity.charging_connector import ChargingConnector
from carconnectivity.enums import ConnectionState
from carconnectivity.climatization import Climatization

from carconnectivity_connectors.base.connector import BaseConnector
from carconnectivity_connectors.renaultdacia.auth.gigya_session import GigyaSession
from carconnectivity_connectors.renaultdacia.vehicle import RenaultVehicle, RenaultElectricVehicle, RenaultCombustionVehicle, \
    RenaultHybridVehicle
from carconnectivity_connectors.renaultdacia.climatization import RenaultClimatization, mapping_renault_climatization_state
from carconnectivity_connectors.renaultdacia.charging import RenaultCharging, mapping_renault_charging_state, mapping_renault_plug_state
from carconnectivity_connectors.renaultdacia._version import __version__

if TYPE_CHECKING:
    from typing import Dict, List, Optional

    from carconnectivity.carconnectivity import CarConnectivity

LOG: logging.Logger = logging.getLogger("carconnectivity.connectors.renaultdacia")
LOG_API: logging.Logger = logging.getLogger("carconnectivity.connectors.renaultdacia-api-debug")

# Locale to API key mapping (Gigya + Kamereon)
GIGYA_URL_EU = "https://accounts.eu1.gigya.com"
GIGYA_URL_US = "https://accounts.us1.gigya.com"
KAMEREON_APIKEY = "YjkKtHmGfaceeuExUDKGxrLZGGvtVS0J"
KAMEREON_URL_EU = "https://api-wired-prod-1-euw1.wrd-aws.com"
KAMEREON_URL_US = "https://api-wired-prod-1-usw2.wrd-aws.com"

AVAILABLE_LOCALES: Dict[str, Dict[str, str]] = {
    "bg_BG": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3__3ER_6lFvXEXHTP_faLtq6eEdbKDXd9F5GoKwzRyZq37ZQ-db7mXcLzR1Jtls5sn",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "cs_CZ": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_oRlKr5PCVL_sPWUZdJ8c5NOl5Ej8nIZw7VKG7S9Rg36UkDszFzfHfxCaUAUU5or2",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "da_DK": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_5x-2C8b1R4MJPQXkwTPdIqgBpcw653Dakw_ZaEneQRkTBdg9UW9Qg_5G-tMNrTMc",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "de_DE": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_VgdkgtIRH3AdHvJm-cjV2ug2EFE0lxt0IJzMC4MFqZjFpn_GYFXVdNZ19L7wZX0N",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "de_AT": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3__B4KghyeUb0GlpU62ZXKrjSfb7CPzwBS368wioftJUL5qXE0Z_sSy0rX69klXuHy",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "de_CH": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_UyiWZs_1UXYCUqK_1n7l7l44UiI_9N9hqwtREV0-UYA_5X7tOV-VKvnGxPBww4q2",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "en_GB": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_e8d4g4SE_Fo8ahyHwwP7ohLGZ79HKNN2T8NjQqoNnk6Epj6ilyYwKdHUyCw3wuxz",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "en_IE": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_Xn7tuOnT9raLEXuwSI1_sFFZNEJhSD0lv3gxkwFtGI-RY4AgiePBiJ9EODh8d9yo",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "es_ES": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_DyMiOwEaxLcPdBTu63Gv3hlhvLaLbW3ufvjHLeuU8U5bx3zx19t5rEKq7KMwk9f1",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "es_MX": {"gigya_url": GIGYA_URL_US, "gigya_api_key": "3_BFzR-2wfhMhUs5OCy3R8U8IiQcHS-81vF8bteSe8eFrboMTjEWzbf4pY1aHQ7cW0",
              "kamereon_url": KAMEREON_URL_US, "kamereon_api_key": KAMEREON_APIKEY},
    "fi_FI": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_xSRCLDYhk1SwSeYQLI3DmA8t-etfAfu5un51fws125ANOBZHgh8Lcc4ReWSwaqNY",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "fr_FR": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_4LKbCcMMcvjDm3X89LU4z4mNKYKdl_W0oD9w-Jvih21WqgJKtFZAnb9YdUgWT9_a",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "fr_BE": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_ZK9x38N8pzEvdiG7ojWHeOAAej43APkeJ5Av6VbTkeoOWR4sdkRc-wyF72HzUB8X",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "fr_CH": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_h3LOcrKZ9mTXxMI9clb2R1VGAWPke6jMNqMw4yYLz4N7PGjYyD0hqRgIFAIHusSn",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "fr_LU": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_zt44Wl_wT9mnqn-BHrR19PvXj3wYRPQKLcPbGWawlatFR837KdxSZZStbBTDaqnb",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "hr_HR": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_HcDC5GGZ89NMP1jORLhYNNCcXt7M3thhZ85eGrcQaM2pRwrgrzcIRWEYi_36cFj9",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "hu_HU": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_nGDWrkSGZovhnVFv5hdIxyuuCuJGZfNmlRGp7-5kEn9yb0bfIfJqoDa2opHOd3Mu",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "it_IT": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_js8th3jdmCWV86fKR3SXQWvXGKbHoWFv8NAgRbH7FnIBsi_XvCpN_rtLcI07uNuq",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "it_CH": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_gHkmHaGACxSLKXqD_uDDx415zdTw7w8HXAFyvh0qIP0WxnHPMF2B9K_nREJVSkGq",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "nl_NL": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_ZIOtjqmP0zaHdEnPK7h1xPuBYgtcOyUxbsTY8Gw31Fzy7i7Ltjfm-hhPh23fpHT5",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "nl_BE": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_yachztWczt6i1pIMhLIH9UA6DXK6vXXuCDmcsoA4PYR0g35RvLPDbp49YribFdpC",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "no_NO": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_QrPkEJr69l7rHkdCVls0owC80BB4CGz5xw_b0gBSNdn3pL04wzMBkcwtbeKdl1g9",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "pl_PL": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_2YBjydYRd1shr6bsZdrvA9z7owvSg3W5RHDYDp6AlatXw9hqx7nVoanRn8YGsBN8",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "pt_PT": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3__afxovspi2-Ip1E5kNsAgc4_35lpLAKCF6bq4_xXj2I2bFPjIWxAOAQJlIkreKTD",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "ro_RO": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_WlBp06vVHuHZhiDLIehF8gchqbfegDJADPQ2MtEsrc8dWVuESf2JCITRo5I2CIxs",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "ru_RU": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_N_ecy4iDyoRtX8v5xOxewwZLKXBjRgrEIv85XxI0KJk8AAdYhJIi17LWb086tGXR",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "sk_SK": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_e8d4g4SE_Fo8ahyHwwP7ohLGZ79HKNN2T8NjQqoNnk6Epj6ilyYwKdHUyCw3wuxz",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "sl_SI": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_QKt0ADYxIhgcje4F3fj9oVidHsx3JIIk-GThhdyMMQi8AJR0QoHdA62YArVjbZCt",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
    "sv_SE": {"gigya_url": GIGYA_URL_EU, "gigya_api_key": "3_EN5Hcnwanu9_Dqot1v1Aky1YelT5QqG4TxveO0EgKFWZYu03WkeB9FKuKKIWUXIS",
              "kamereon_url": KAMEREON_URL_EU, "kamereon_api_key": KAMEREON_APIKEY},
}

# Kamereon endpoint URL patterns
KAMEREON_COMMERCE_URL = "{kamereon_root_url}/commerce/v1"
KAMEREON_PERSON_URL = "{kamereon_root_url}/commerce/v1/persons/{person_id}"
KAMEREON_VEHICLES_URL = "{kamereon_root_url}/commerce/v1/accounts/{account_id}/vehicles"
KAMEREON_VEHICLE_DATA_URL = (
    "{kamereon_root_url}/commerce/v1/accounts/{account_id}/kamereon/kca/car-adapter/v{version}/cars/{vin}/{endpoint}"
)
# pah: KCM endpoints, e.g. EV SOC target
KAMEREON_KCM_VEHICLE_DATA_URL = (
    "{kamereon_root_url}/commerce/v1/accounts/{account_id}/kamereon/kcm/v1/vehicles/{vin}/{endpoint}"
)
KAMEREON_VEHICLE_ACTION_URL = (
    "{kamereon_root_url}/commerce/v1/accounts/{account_id}/kamereon/kca/car-adapter/v{version}/cars/{vin}/{endpoint}"
)


# pylint: disable=too-many-lines
class Connector(BaseConnector):
    """
    Connector class for Renault/Dacia API connectivity.

    Args:
        car_connectivity (CarConnectivity): An instance of CarConnectivity.
        config (Dict): Configuration dictionary containing connection details.
    """
    def __init__(self, connector_id: str, car_connectivity: CarConnectivity, config: Dict, *args,  # pylint: disable=too-many-branches,too-many-statements
                 initialization: Optional[Dict] = None, **kwargs) -> None:
        BaseConnector.__init__(self, connector_id=connector_id, car_connectivity=car_connectivity, config=config,
                               log=LOG, api_log=LOG_API, *args, initialization=initialization, **kwargs)

        self._background_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self.connection_state: EnumAttribute[ConnectionState] = EnumAttribute(
            name="connection_state", parent=self, value_type=ConnectionState,
            value=ConnectionState.DISCONNECTED, tags={'connector_custom'}
        )
        self.interval: DurationAttribute = DurationAttribute(name="interval", parent=self, tags={'connector_custom'})
        self.interval.minimum = timedelta(seconds=300)
        self.interval._is_changeable = True  # pylint: disable=protected-access

        LOG.info("Loading renaultdacia connector with config %s", config_remove_credentials(config))

        # Locale
        self.active_config['locale'] = config.get('locale', 'de_DE')
        if self.active_config['locale'] not in AVAILABLE_LOCALES:
            raise ValueError(f"Unsupported locale '{self.active_config['locale']}'. Supported locales: {list(AVAILABLE_LOCALES.keys())}")

        locale_config = AVAILABLE_LOCALES[self.active_config['locale']]
        country = self.active_config['locale'].split('_')[1]

        # Username / password
        self.active_config['username'] = None
        self.active_config['password'] = None
        if 'username' in config and 'password' in config:
            self.active_config['username'] = config['username']
            self.active_config['password'] = config['password']
        else:
            if 'netrc' in config:
                self.active_config['netrc'] = config['netrc']
            else:
                self.active_config['netrc'] = os.path.join(os.path.expanduser("~"), ".netrc")
            try:
                secrets = netrc.netrc(file=self.active_config['netrc'])
                secret: tuple[str, str, str] | None = secrets.authenticators("renaultdacia")
                if secret is None:
                    raise AuthenticationError(
                        f'Authentication using {self.active_config["netrc"]} failed: renaultdacia not found in netrc'
                    )
                self.active_config['username'], _, self.active_config['password'] = secret
            except netrc.NetrcParseError as err:
                LOG.error('Authentication using %s failed: %s', self.active_config['netrc'], err)
                raise AuthenticationError(
                    f'Authentication using {self.active_config["netrc"]} failed: {err}'
                ) from err
            except TypeError as err:
                if 'username' not in config:
                    raise AuthenticationError(
                        f'"renaultdacia" entry was not found in {self.active_config["netrc"]} netrc-file.'
                        ' Create it or provide username and password in config'
                    ) from err
            except FileNotFoundError as err:
                raise AuthenticationError(
                    f'{self.active_config["netrc"]} netrc-file was not found.'
                    ' Create it or provide username and password in config'
                ) from err

        if self.active_config['username'] is None or self.active_config['password'] is None:
            raise AuthenticationError('Username or password not provided')

        # Interval
        self.active_config['interval'] = 300
        if 'interval' in config:
            self.active_config['interval'] = config['interval']
            if self.active_config['interval'] < 300:
                raise ValueError('Interval must be at least 300 seconds')
        self.active_config['max_age'] = self.active_config['interval'] - 1
        if 'max_age' in config:
            self.active_config['max_age'] = config['max_age']
        if 'max_age_static' in config:
            self.active_config['max_age_static'] = config['max_age_static']
        else:
            self.active_config['max_age_static'] = 86400  # 24 hours
        self.interval._set_value(timedelta(seconds=self.active_config['interval']))  # pylint: disable=protected-access

        # Set up Gigya session
        tokenstore = car_connectivity.get_tokenstore()
        token_key = f"carconnectivity-connector-renaultdacia:{self.active_config['username']}"
        saved_tokens = tokenstore.get(token_key)

        self.session: GigyaSession = GigyaSession(
            username=self.active_config['username'],
            password=self.active_config['password'],
            gigya_root_url=locale_config['gigya_url'],
            gigya_api_key=locale_config['gigya_api_key'],
            kamereon_root_url=locale_config['kamereon_url'],
            kamereon_api_key=locale_config['kamereon_api_key'],
            country=country,
            token_store=saved_tokens,
        )
        self._token_key = token_key

        # Perform initial login
        try:
            self.session.login()
        except (AuthenticationError, TemporaryAuthenticationError) as err:
            raise AuthenticationError(f'There was a problem when authenticating with one or multiple services: {err}') from err

        self._elapsed: List[timedelta] = []

        # pah: account preference / fallback.
        # Renault returns several logical accounts for one person.  For the
        # observed Dacia account the person-owned SFDC account provides the
        # vehicle data and MYDACIA can be used as a fallback.  Keeping both
        # IDs here allows endpoint-level fallback without performing every
        # successful request twice.
        self._preferred_account_id = None
        self._fallback_account_id = None

        # pah: experimental endpoints must not be queried every poll cycle.
        # Key: "<vin>:<endpoint>", value: timestamp of last attempt.
        self._experimental_last_attempt = {}

    def startup(self) -> None:
        """Start the background polling thread."""
        self._background_thread = threading.Thread(target=self._background_loop, daemon=False)
        self._background_thread.name = 'carconnectivity.connectors.renaultdacia-background'
        self._background_thread.start()
        self.healthy._set_value(value=True)  # pylint: disable=protected-access

    def _background_loop(self) -> None:
        self._stop_event.clear()
        fetch: bool = True
        self.connection_state._set_value(value=ConnectionState.CONNECTING)  # pylint: disable=protected-access
        while not self._stop_event.is_set():
            interval = self.active_config['interval']
            try:
                try:
                    if fetch:
                        self.fetch_all()
                        fetch = False
                except Exception:  # pylint: disable=broad-except
                    LOG.critical('There was an unexpected exception: %s', traceback.format_exc())
                    self.connection_state._set_value(value=ConnectionState.ERROR)  # pylint: disable=protected-access
                    fetch = True
            finally:
                self._stop_event.wait(interval)
                fetch = True



    def fetch_all(self) -> None:  # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        """Fetch all data from the Renault API."""
        self.connection_state._set_value(value=ConnectionState.CONNECTED)  # pylint: disable=protected-access
        start_time = datetime.now(tz=timezone.utc)

        self._persist_tokens()

        try:
            person_id = self.session.get_person_id()
        except (AuthenticationError, TemporaryAuthenticationError) as err:
            self.connection_state._set_value(value=ConnectionState.ERROR)  # pylint: disable=protected-access
            raise RetrievalError(f'Failed to retrieve person ID: {err}') from err

        try:
            person_url = KAMEREON_PERSON_URL.format(
                kamereon_root_url=self.session.kamereon_root_url,
                person_id=person_id,
            )
            person_data = self.session.kamereon_get(person_url)
            LOG_API.debug("Person data: %s", json.dumps(person_data, indent=2))
        except requests.exceptions.HTTPError as err:
            self._handle_http_error(err)
            return
        except (AuthenticationError, TemporaryAuthenticationError) as err:
            self.connection_state._set_value(value=ConnectionState.ERROR)  # pylint: disable=protected-access
            raise RetrievalError(f'Failed to retrieve person data: {err}') from err

        # pah:
        # Renault returns several ACTIVE/OWNER accounts for the same person.
        #
        # Preferred:
        #   SFDC       -> person-owned account (Rebecca Kuspiel in observed data)
        # Fallback:
        #   MYDACIA    -> only when SFDC cannot supply the required information
        #
        # SALES_LOCAL is not used for connected-car telemetry.
        accounts = person_data.get('accounts', []) if isinstance(person_data, dict) else []
        active_owner_accounts = [
            account for account in accounts
            if account.get('accountId')
            and account.get('accountStatus') == 'ACTIVE'
            and account.get('relationType') == 'OWNER'
        ]

        preferred_account = next(
            (account for account in active_owner_accounts if account.get('accountType') == 'SFDC'),
            None
        )
        fallback_account = next(
            (account for account in active_owner_accounts if account.get('accountType') == 'MYDACIA'),
            None
        )

        if preferred_account is None:
            preferred_account = fallback_account
        if preferred_account is None and active_owner_accounts:
            preferred_account = active_owner_accounts[0]

        self._preferred_account_id = preferred_account.get('accountId') if preferred_account else None
        self._fallback_account_id = fallback_account.get('accountId') if fallback_account else None
        if self._fallback_account_id == self._preferred_account_id:
            self._fallback_account_id = None

        LOG_API.debug(
            "Renault account selection for %s %s: preferred_type=%s fallback_type=%s",
            person_data.get('firstName', '') if isinstance(person_data, dict) else '',
            person_data.get('lastName', '') if isinstance(person_data, dict) else '',
            preferred_account.get('accountType') if preferred_account else None,
            fallback_account.get('accountType') if fallback_account else None,
        )

        if not self._preferred_account_id:
            LOG.warning("No usable Renault account found for person %s", person_id)
            return

        garage: Garage = self.car_connectivity.garage

        # --------------------------------------------------------------
        # 1. Vehicle list from the preferred SFDC account
        # --------------------------------------------------------------
        try:
            vehicles_url = KAMEREON_VEHICLES_URL.format(
                kamereon_root_url=self.session.kamereon_root_url,
                account_id=self._preferred_account_id,
            )
            vehicles_data = self.session.kamereon_get(vehicles_url)
            LOG_API.debug(
                "Vehicles data for preferred account %s: %s",
                self._preferred_account_id,
                json.dumps(vehicles_data, indent=2)
            )
            vehicle_links = vehicles_data.get('vehicleLinks', [])
        except requests.exceptions.HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            if status == 429:
                raise TooManyRequestsError('Too many requests to the Renault API') from err
            LOG_API.debug(
                "Preferred Renault account could not supply vehicle list (HTTP %s)",
                status
            )
            vehicle_links = []
        except (AuthenticationError, TemporaryAuthenticationError) as err:
            LOG_API.debug("Preferred Renault account could not supply vehicle list: %s", err)
            vehicle_links = []

        # If the preferred account failed completely, fall back to MYDACIA.
        selected_account_id = self._preferred_account_id
        if not vehicle_links and self._fallback_account_id:
            try:
                vehicles_url = KAMEREON_VEHICLES_URL.format(
                    kamereon_root_url=self.session.kamereon_root_url,
                    account_id=self._fallback_account_id,
                )
                vehicles_data = self.session.kamereon_get(vehicles_url)
                LOG_API.debug(
                    "Vehicles data for MYDACIA fallback account %s: %s",
                    self._fallback_account_id,
                    json.dumps(vehicles_data, indent=2)
                )
                vehicle_links = vehicles_data.get('vehicleLinks', [])
                if vehicle_links:
                    selected_account_id = self._fallback_account_id
            except requests.exceptions.HTTPError as err:
                status = err.response.status_code if err.response is not None else None
                if status == 429:
                    raise TooManyRequestsError('Too many requests to the Renault API') from err
                LOG_API.debug(
                    "MYDACIA fallback could not supply vehicle list (HTTP %s)",
                    status
                )
            except (AuthenticationError, TemporaryAuthenticationError) as err:
                LOG_API.debug("MYDACIA fallback could not supply vehicle list: %s", err)

        if not vehicle_links:
            LOG.warning("No Renault/Dacia vehicles found for the available accounts")
            return

        # --------------------------------------------------------------
        # 2. SFDC may know the VIN but return incomplete vehicleDetails.
        #
        # In particular, if energy.code is missing we cannot safely decide
        # whether to instantiate RenaultElectricVehicle.  Without that class,
        # _fetch_battery_status() is skipped and MQTT loses:
        #
        #   drives/electric/level
        #   drives/electric/range
        #   charging/estimated_date_reached
        #
        # Therefore query MYDACIA *only* for missing vehicle metadata. This is
        # not a duplicate telemetry request. Battery/HVAC/location are still
        # fetched once from the preferred account, with endpoint-level MYDACIA
        # fallback only if the preferred request fails.
        # --------------------------------------------------------------
        needs_metadata_fallback = False
        if (
                selected_account_id == self._preferred_account_id
                and self._fallback_account_id
        ):
            for vehicle_link in vehicle_links:
                details = vehicle_link.get('vehicleDetails', {})
                energy = details.get('energy', {}).get('code', '')
                if not energy:
                    needs_metadata_fallback = True
                    break

        fallback_details_by_vin = {}
        if needs_metadata_fallback:
            try:
                fallback_url = KAMEREON_VEHICLES_URL.format(
                    kamereon_root_url=self.session.kamereon_root_url,
                    account_id=self._fallback_account_id,
                )
                fallback_data = self.session.kamereon_get(fallback_url)
                LOG_API.debug(
                    "MYDACIA vehicle metadata fallback: %s",
                    json.dumps(fallback_data, indent=2)
                )
                for fallback_link in fallback_data.get('vehicleLinks', []):
                    fallback_vin = fallback_link.get('vin')
                    if fallback_vin:
                        fallback_details_by_vin[fallback_vin.upper()] = \
                            fallback_link.get('vehicleDetails', {})
            except requests.exceptions.HTTPError as err:
                status = err.response.status_code if err.response is not None else None
                LOG_API.debug(
                    "MYDACIA vehicle metadata fallback unavailable (HTTP %s)",
                    status
                )
            except (AuthenticationError, TemporaryAuthenticationError) as err:
                LOG_API.debug("MYDACIA vehicle metadata fallback failed: %s", err)

        for vehicle_link in vehicle_links:
            vin = vehicle_link.get('vin')
            if not vin:
                continue

            vehicle_details = dict(vehicle_link.get('vehicleDetails', {}) or {})

            # Fill only missing metadata; never overwrite data delivered by SFDC.
            fallback_details = fallback_details_by_vin.get(vin.upper())
            if fallback_details:
                if not vehicle_details.get('energy', {}).get('code'):
                    if fallback_details.get('energy'):
                        vehicle_details['energy'] = fallback_details['energy']

                if not vehicle_details.get('brand') and fallback_details.get('brand'):
                    vehicle_details['brand'] = fallback_details['brand']

                if not vehicle_details.get('model') and fallback_details.get('model'):
                    vehicle_details['model'] = fallback_details['model']

                if not vehicle_details.get('registrationPlate') and fallback_details.get('registrationPlate'):
                    vehicle_details['registrationPlate'] = fallback_details['registrationPlate']

                if not vehicle_details.get('licencePlate') and fallback_details.get('licencePlate'):
                    vehicle_details['licencePlate'] = fallback_details['licencePlate']

            LOG_API.debug(
                "Effective vehicleDetails for %s: %s",
                vin,
                json.dumps(vehicle_details, indent=2)
            )

            self._fetch_vehicle(
                garage,
                selected_account_id,
                vin,
                vehicle_details
            )

        elapsed = datetime.now(tz=timezone.utc) - start_time
        self._elapsed.append(elapsed)
        LOG.debug("Fetching all data took %s", elapsed)

    def _persist_tokens(self) -> None:
        """Persist session tokens to the token store."""
        tokenstore = self.car_connectivity.get_tokenstore()
        tokenstore[self._token_key] = self.session.save_to_token_store()


    def _handle_http_error(self, err: requests.exceptions.HTTPError) -> None:
        """Handle HTTP errors from the API."""
        if err.response is not None:
            status_code = err.response.status_code
            if status_code == 429:
                raise TooManyRequestsError('Too many requests to the Renault API') from err
            if status_code in (401, 403):
                self.connection_state._set_value(value=ConnectionState.ERROR)  # pylint: disable=protected-access
                raise AuthenticationError(f'Authentication failed with status {status_code}') from err
        raise RetrievalError(f'HTTP error: {err}') from err

    def _vehicle_account_candidates(self, account_id: str) -> List[str]:
        """Return the preferred account followed by MYDACIA fallback, without duplicates."""
        result = [account_id]
        if (
                self._fallback_account_id
                and self._fallback_account_id != account_id
                and account_id == self._preferred_account_id
        ):
            result.append(self._fallback_account_id)
        return result

    def _kamereon_get_vehicle_data(
            self,
            account_id: str,
            vin: str,
            endpoint: str,
            version: int = 1
    ):
        """
        Read a known-good KCA vehicle endpoint.

        pah: Do not duplicate successful requests.  The preferred SFDC account is
        tried first; MYDACIA is queried only if the preferred account cannot
        deliver this endpoint.
        """
        candidates = self._vehicle_account_candidates(account_id)
        last_error = None

        for index, candidate in enumerate(candidates):
            url = self._get_vehicle_data_url(candidate, vin, endpoint, version=version)
            try:
                return self.session.kamereon_get(url), candidate
            except requests.exceptions.HTTPError as err:
                last_error = err
                status = err.response.status_code if err.response is not None else None

                # Do not multiply quota requests.
                if status == 429:
                    raise

                has_fallback = index + 1 < len(candidates)
                if has_fallback and status in (403, 404, 500, 502, 503, 504):
                    LOG_API.debug(
                        "%s unavailable for %s via preferred account (HTTP %s); trying MYDACIA fallback",
                        endpoint,
                        vin,
                        status,
                    )
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RetrievalError(f'No account available for endpoint {endpoint} and vehicle {vin}')

    @staticmethod
    def _set_custom_string(parent, name: str, value) -> None:
        """Create/update a connector_custom StringAttribute so MQTT publishes the value."""
        if parent is None or value is None:
            return
        attribute = getattr(parent, name, None)
        if attribute is None:
            attribute = StringAttribute(
                name=name,
                parent=parent,
                tags={'connector_custom'}
            )
            setattr(parent, name, attribute)
        attribute._set_value(value=str(value))  # pylint: disable=protected-access

    @staticmethod
    def _set_custom_duration(parent, name: str, value: timedelta) -> None:
        """Create/update a connector_custom DurationAttribute so MQTT publishes the value."""
        if parent is None or value is None:
            return
        attribute = getattr(parent, name, None)
        if attribute is None:
            attribute = DurationAttribute(
                name=name,
                parent=parent,
                tags={'connector_custom'}
            )
            setattr(parent, name, attribute)
        attribute._set_value(value=value)  # pylint: disable=protected-access

    def _experimental_due(self, vin: str, endpoint: str, minimum_age: timedelta) -> bool:
        """Throttle endpoints that are still experimental or incompletely understood."""
        key = f"{vin}:{endpoint}"
        now = datetime.now(tz=timezone.utc)
        last_attempt = self._experimental_last_attempt.get(key)
        if last_attempt is not None and now - last_attempt < minimum_age:
            return False
        self._experimental_last_attempt[key] = now
        return True


    def _fetch_vehicle(self, garage: Garage, account_id: str, vin: str, vehicle_details: Dict) -> None:  # pylint: disable=too-many-branches,too-many-statements
        """Fetch and populate data for a single vehicle."""
        brand = vehicle_details.get('brand', {}).get('label', 'Renault')
        model = vehicle_details.get('model', {}).get('label', '')
        energy = vehicle_details.get('energy', {}).get('code', '')

        LOG.debug("Processing vehicle VIN=%s brand=%s model=%s energy=%s", vin, brand, model, energy)

        vehicle: Optional[RenaultVehicle] = None

        # pah: Reuse an already matching Renault vehicle. If another connector
        # already created the VIN with a different/generic vehicle class, wrap
        # that object via origin= and replace it in the garage. This preserves
        # existing data while creating Renault-specific climatization/charging.
        existing_vehicle = garage.get_vehicle(vin)

        # If the later account response contains no/unknown energy value, keep
        # the already established Renault type rather than downgrading an EV.
        is_known_electric_model = (
            brand.strip().upper() == 'DACIA'
            and model.strip().upper().startswith('SPRING')
        )

        if energy in ('ELEC', 'ELECTRIC') or is_known_electric_model:
            if isinstance(existing_vehicle, RenaultElectricVehicle):
                vehicle = existing_vehicle
            else:
                vehicle = RenaultElectricVehicle(
                    vin=vin if existing_vehicle is None else None,
                    garage=garage,
                    managing_connector=self,
                    origin=existing_vehicle
                )
                if existing_vehicle is None:
                    garage.add_vehicle(vin, vehicle)
                else:
                    garage.replace_vehicle(vin, vehicle)

        elif energy in ('HEV', 'PHEV', 'HYBRID'):
            if isinstance(existing_vehicle, RenaultHybridVehicle):
                vehicle = existing_vehicle
            else:
                vehicle = RenaultHybridVehicle(
                    vin=vin if existing_vehicle is None else None,
                    garage=garage,
                    managing_connector=self,
                    origin=existing_vehicle
                )
                if existing_vehicle is None:
                    garage.add_vehicle(vin, vehicle)
                else:
                    garage.replace_vehicle(vin, vehicle)

        elif isinstance(existing_vehicle, RenaultVehicle):
            vehicle = existing_vehicle

        else:
            vehicle = RenaultCombustionVehicle(
                vin=vin if existing_vehicle is None else None,
                garage=garage,
                managing_connector=self,
                origin=existing_vehicle
            )
            if existing_vehicle is None:
                garage.add_vehicle(vin, vehicle)
            else:
                garage.replace_vehicle(vin, vehicle)

        if vehicle is None:
            return

        vehicle.manufacturer._set_value(value=brand)  # pylint: disable=protected-access
        if model:
            vehicle.model._set_value(value=model)  # pylint: disable=protected-access

        license_plate = vehicle_details.get('registrationPlate') or vehicle_details.get('licencePlate')
        if license_plate and vehicle.license_plate is not None:
            vehicle.license_plate._set_value(value=license_plate)  # pylint: disable=protected-access

        LOG_API.debug(
            "Vehicle %s instantiated as %s (energy=%s)",
            vin,
            type(vehicle).__name__,
            energy,
        )

        # ------------------------------------------------------------------
        # Confirmed working endpoints for the Dacia Spring
        # ------------------------------------------------------------------
        self._fetch_cockpit(account_id, vin, vehicle)

        if isinstance(vehicle, (RenaultElectricVehicle, RenaultHybridVehicle)):
            self._fetch_battery_status(account_id, vin, vehicle)

        self._fetch_hvac_status(account_id, vin, vehicle)
        self._fetch_location(account_id, vin, vehicle)

        # ------------------------------------------------------------------
        # Confirmed NOT useful for this Spring -- deliberately NOT queried.
        #
        # KCA charge-mode        -> HTTP 403
        # KCA charge-schedule    -> HTTP 403
        # KCA charging-settings  -> HTTP 403
        # KCA lock-status        -> HTTP 404 (no data for VIN/account)
        # KCA pressure           -> HTTP 404 (no data for VIN/account)
        # KCA charge-history     -> HTTP 404 ("specified url does not exist")
        # KCM ev/soc-level       -> HTTP 404
        #
        # Keeping this list here documents the tests without burning API quota.
        # ------------------------------------------------------------------

        # Still unresolved / experimental:
        # - KCA charges: endpoint exists, but current parameterization returns
        #   HTTP 400 conversionFailed.
        # - KCM ev/settings: endpoint support is unresolved; tests reached HTTP
        #   429 quota limit rather than 403/404.
        if isinstance(vehicle, (RenaultElectricVehicle, RenaultHybridVehicle)):
            self._fetch_experimental_ev_data(account_id, vin, vehicle)

    def _get_vehicle_data_url(self, account_id: str, vin: str, endpoint: str, version: int = 1) -> str:
        """Build URL for a Kamereon vehicle data endpoint."""
        return KAMEREON_VEHICLE_DATA_URL.format(
            kamereon_root_url=self.session.kamereon_root_url,
            account_id=account_id,
            version=version,
            vin=vin,
            endpoint=endpoint,
        )
        

    # pah: temporary Renault endpoint probe

    def _fetch_experimental_ev_data(
            self,
            account_id: str,
            vin: str,
            vehicle: RenaultElectricVehicle
    ) -> None:
        """
        Probe only endpoints whose support is still unresolved.

        These requests are deliberately throttled and use only the already
        selected account.  They do not fall back to MYDACIA, because duplicate
        tests previously produced the same result and unnecessarily consumed
        the Renault quota.
        """

        # KCA charges -------------------------------------------------------
        # The endpoint exists, but our parameter format still returns:
        # HTTP 400 err.func.wired.conversionFailed.
        # Keep one attempt per 24 h until the correct parameter format is known.
        if self._experimental_due(vin, 'charges', timedelta(hours=2)):
            now = datetime.now(timezone.utc)
            start_date = now - timedelta(days=30)
            start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
            end_str = now.strftime('%Y-%m-%dT23:59:59Z')

            url = self._get_vehicle_data_url(account_id, vin, 'charges', version=1)
            url += f"?start={start_str}&end={end_str}"

            try:
                data = self.session.kamereon_get(url)
                LOG_API.debug("Experimental charges data for %s: %s", vin, json.dumps(data, indent=2))
                self._map_charges_data(vehicle, data)
            except requests.exceptions.HTTPError as err:
                status = err.response.status_code if err.response is not None else None
                response_text = err.response.text if err.response is not None else ""
                LOG_API.debug(
                    "Experimental endpoint charges for %s remains unresolved "
                    "(current parameterization; HTTP %s): %s",
                    vin,
                    status,
                    response_text,
                )
            except Exception as err:  # pylint: disable=broad-except
                LOG_API.debug("Experimental endpoint charges failed for %s: %s", vin, err)

        # KCM ev/settings ---------------------------------------------------
        # This endpoint has NOT been disproved by 403/404.  During testing the
        # server answered HTTP 429 quota exceeded, so support remains unresolved.
        # Keep one attempt per 6 h.
        if self._experimental_due(vin, 'ev/settings', timedelta(hours=2)):
            url = KAMEREON_KCM_VEHICLE_DATA_URL.format(
                kamereon_root_url=self.session.kamereon_root_url,
                account_id=account_id,
                vin=vin,
                endpoint='ev/settings'
            )

            try:
                data = self.session.kamereon_get(url)
                LOG_API.debug("Experimental ev/settings data for %s: %s", vin, json.dumps(data, indent=2))
                self._map_ev_settings(vehicle, data)
            except requests.exceptions.HTTPError as err:
                status = err.response.status_code if err.response is not None else None
                response_text = err.response.text if err.response is not None else ""
                LOG_API.debug(
                    "Experimental endpoint ev/settings for %s remains unresolved "
                    "(HTTP %s): %s",
                    vin,
                    status,
                    response_text,
                )
            except Exception as err:  # pylint: disable=broad-except
                LOG_API.debug("Experimental endpoint ev/settings failed for %s: %s", vin, err)

    def _map_charges_data(self, vehicle: RenaultElectricVehicle, data) -> None:
        """Expose the latest charge session if the experimental charges endpoint starts working."""
        if not isinstance(data, dict) or vehicle.charging is None:
            return

        payload = data.get('data', data)
        if isinstance(payload, dict):
            attributes = payload.get('attributes', payload)
        else:
            attributes = {}

        charges = attributes.get('charges') if isinstance(attributes, dict) else None
        if not isinstance(charges, list) or not charges:
            return

        latest = charges[-1]
        if not isinstance(latest, dict):
            return

        mapping = {
            'chargeStartDate': 'last_charge_start',
            'chargeEndDate': 'last_charge_end',
            'chargeDuration': 'last_charge_duration_raw',
            'chargeStartBatteryLevel': 'last_charge_start_level',
            'chargeEndBatteryLevel': 'last_charge_end_level',
            'chargeBatteryLevelRecovered': 'last_charge_level_recovered',
            'chargePower': 'last_charge_power_type',
            'chargeStartInstantaneousPower': 'last_charge_start_power',
            'chargeEndStatus': 'last_charge_end_status',
        }
        for source_name, target_name in mapping.items():
            self._set_custom_string(vehicle.charging, target_name, latest.get(source_name))

    def _map_ev_settings(self, vehicle: RenaultElectricVehicle, data) -> None:
        """Expose KCM EV settings if the currently unresolved endpoint becomes available."""
        if not isinstance(data, dict):
            return

        payload = data.get('data', data)
        if isinstance(payload, dict):
            attributes = payload.get('attributes', payload)
        else:
            attributes = {}

        if not isinstance(attributes, dict):
            return

        if vehicle.charging is not None and vehicle.charging.settings is not None:
            settings = vehicle.charging.settings
            for source_name, target_name in {
                'lastSettingsUpdateTimestamp': 'last_settings_update',
                'delegatedActivated': 'delegated_activated',
                'chargeModeRq': 'charge_mode_request',
                'chargeTimeStart': 'charge_time_start',
                'chargeDuration': 'charge_duration_raw',
            }.items():
                self._set_custom_string(settings, target_name, attributes.get(source_name))

        if vehicle.climatization is not None:
            for source_name, target_name in {
                'preconditioningTemperature': 'preconditioning_temperature',
                'preconditioningHeatedStrgWheel': 'preconditioning_heated_steering_wheel',
                'preconditioningHeatedRightSeat': 'preconditioning_heated_right_seat',
                'preconditioningHeatedLeftSeat': 'preconditioning_heated_left_seat',
            }.items():
                self._set_custom_string(vehicle.climatization, target_name, attributes.get(source_name))

            if attributes.get('programs') is not None:
                self._set_custom_string(
                    vehicle.climatization,
                    'preconditioning_programs',
                    json.dumps(attributes.get('programs'), separators=(',', ':'))
                )


    def _fetch_cockpit(self, account_id: str, vin: str, vehicle: RenaultVehicle) -> None:  # pylint: disable=too-many-branches
        """Fetch cockpit data (odometer, fuel level) for a vehicle."""
        try:
            data, used_account_id = self._kamereon_get_vehicle_data(
                account_id, vin, 'cockpit', version=1
            )
            LOG_API.debug(
                "Cockpit data for %s via account %s: %s",
                vin,
                used_account_id,
                json.dumps(data, indent=2)
            )
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code in (404, 501):
                LOG_API.debug("Cockpit endpoint not available for %s", vin)
                return
            LOG.warning("Failed to fetch cockpit data for %s: %s", vin, err)
            return
        except Exception as err:  # pylint: disable=broad-except
            LOG.warning("Failed to fetch cockpit data for %s: %s", vin, err)
            return

        attributes = data.get('data', {}).get('attributes', {})
        if not attributes:
            log_extra_keys(LOG, 'cockpit', data, {'data'})
            return

        total_mileage = attributes.get('totalMileage')
        if total_mileage is not None and vehicle.odometer is not None:
            vehicle.odometer._set_value(value=float(total_mileage), unit=Length.KM)  # pylint: disable=protected-access

        if isinstance(vehicle, RenaultCombustionVehicle):
            fuel_autonomy = attributes.get('fuelAutonomy')
            fuel_quantity = attributes.get('fuelQuantity')
            drive: Optional[CombustionDrive] = None
            if vehicle.drives is not None:
                for d in vehicle.drives.drives.values():
                    if isinstance(d, CombustionDrive):
                        drive = d
                        break
            if drive is None and vehicle.drives is not None:
                drive = CombustionDrive(drive_id='combustion', drives=vehicle.drives)
                vehicle.drives.add_drive(drive)
            if drive is not None:
                if fuel_autonomy is not None and drive.range is not None:
                    drive.range._set_value(value=float(fuel_autonomy), unit=Length.KM)  # pylint: disable=protected-access
                if fuel_quantity is not None and drive.fuel_tank is not None and drive.fuel_tank.available_capacity is not None:
                    drive.fuel_tank.available_capacity._set_value(value=float(fuel_quantity), unit=Volume.L)  # pylint: disable=protected-access

        log_extra_keys(LOG, 'cockpit.attributes', attributes,
                       {'totalMileage', 'fuelAutonomy', 'fuelQuantity', 'totalMileageUnit'})


    def _fetch_battery_status(self, account_id: str, vin: str, vehicle: RenaultElectricVehicle) -> None:
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        """Fetch battery/charging status for an electric/hybrid vehicle."""
        try:
            data, used_account_id = self._kamereon_get_vehicle_data(
                account_id, vin, 'battery-status', version=2
            )
            LOG_API.debug(
                "Battery status for %s via account %s: %s",
                vin,
                used_account_id,
                json.dumps(data, indent=2)
            )
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code in (404, 501):
                LOG_API.debug("Battery status endpoint not available for %s", vin)
                return
            LOG.warning("Failed to fetch battery status for %s: %s", vin, err)
            return
        except Exception as err:  # pylint: disable=broad-except
            LOG.warning("Failed to fetch battery status for %s: %s", vin, err)
            return

        attributes = data.get('data', {}).get('attributes', {})
        if not attributes:
            log_extra_keys(LOG, 'battery-status', data, {'data'})
            return

        battery_level = attributes.get('batteryLevel')
        battery_autonomy = attributes.get('batteryAutonomy')
        battery_temperature = attributes.get('batteryTemperature')
        battery_available_energy = attributes.get('batteryAvailableEnergy')
        battery_timestamp = attributes.get('timestamp')
        v2l_status = attributes.get('V2L_SystemStatusDisplay')

        # Get or create electric drive
        drive: Optional[ElectricDrive] = None
        if vehicle.drives is not None:
            for d in vehicle.drives.drives.values():
                if isinstance(d, ElectricDrive):
                    drive = d
                    break
        if drive is None and vehicle.drives is not None:
            drive = ElectricDrive(drive_id='electric', drives=vehicle.drives)
            vehicle.drives.add_drive(drive)

        if drive is not None:
            if battery_level is not None and drive.level is not None:
                drive.level._set_value(value=float(battery_level))

            if battery_autonomy is not None and drive.range is not None:
                drive.range._set_value(value=float(battery_autonomy), unit=Length.KM)

            # Existing positive extensions retained.
            if battery_temperature is not None and drive.battery is not None:
                drive.battery.temperature._set_value(
                    value=float(battery_temperature),
                    unit=Temperature.C
                )

            if battery_available_energy is not None and drive.battery is not None:
                drive.battery.available_capacity._set_value(
                    value=float(battery_available_energy)
                )

            # Additional Spring fields that were actually observed.
            if drive.battery is not None:
                self._set_custom_string(drive.battery, 'last_update', battery_timestamp)
                self._set_custom_string(drive.battery, 'v2l_system_status', v2l_status)

        charging_status = attributes.get('chargingStatus')
        plug_status = attributes.get('plugStatus')
        charging_remaining_time = attributes.get('chargingRemainingTime')
        remaining_time_to_full_charge = attributes.get('remainingTime')
        charging_instantaneous_power = attributes.get('chargingInstantaneousPower')
        remaining_time_last_update = attributes.get('chargingRemainingTimeLastUpdateDateTime')

        if isinstance(vehicle.charging, RenaultCharging):
            charging: RenaultCharging = vehicle.charging

            # Renault/Dacia battery-status uses numeric charge states on the
            # Spring/Zoe API: 0.0=not charging, 1.0=charging, -1.0=error.
            generic_state = None
            try:
                numeric_charging_status = float(charging_status) if charging_status is not None else None
            except (TypeError, ValueError):
                numeric_charging_status = None

            if numeric_charging_status == 1.0:
                generic_state = Charging.ChargingState.CHARGING
            elif numeric_charging_status == 0.0:
                generic_state = Charging.ChargingState.OFF
            elif numeric_charging_status == -1.0:
                generic_state = Charging.ChargingState.ERROR
            elif charging_status is not None:
                # Keep compatibility with string enum values returned by some models.
                try:
                    renault_state = RenaultCharging.RenaultChargingState(charging_status)
                    generic_state = mapping_renault_charging_state.get(
                        renault_state,
                        Charging.ChargingState.UNKNOWN
                    )
                except ValueError:
                    LOG_API.debug("Unknown charging state for %s: %s", vin, charging_status)
                    generic_state = Charging.ChargingState.UNKNOWN

            if generic_state is not None and charging.state is not None:
                charging.state._set_value(value=generic_state)  # pylint: disable=protected-access

            # plugStatus=1 is documented/observed as plugged.  Any other numeric
            # value is treated as unplugged unless a model returns a known string enum.
            generic_plug = None
            if plug_status is not None:
                try:
                    numeric_plug_status = int(float(plug_status))
                except (TypeError, ValueError):
                    numeric_plug_status = None

                if numeric_plug_status == 1:
                    generic_plug = ChargingConnector.ChargingConnectorConnectionState.CONNECTED
                elif numeric_plug_status is not None:
                    generic_plug = ChargingConnector.ChargingConnectorConnectionState.DISCONNECTED
                else:
                    try:
                        renault_plug = RenaultCharging.RenaultPlugState(plug_status)
                        generic_plug = mapping_renault_plug_state.get(
                            renault_plug,
                            ChargingConnector.ChargingConnectorConnectionState.UNKNOWN
                        )
                    except ValueError:
                        LOG_API.debug("Unknown plug state for %s: %s", vin, plug_status)
                        generic_plug = ChargingConnector.ChargingConnectorConnectionState.UNKNOWN

            if (
                    generic_plug is not None
                    and charging.connector is not None
                    and charging.connector.connection_state is not None
            ):
                charging.connector.connection_state._set_value(value=generic_plug)  # pylint: disable=protected-access

            # Preserve raw API values as explicit MQTT topics as well.
            self._set_custom_string(charging, 'renault_charging_status_raw', charging_status)
            if charging.connector is not None:
                self._set_custom_string(charging.connector, 'renault_plug_status_raw', plug_status)

            # Remaining time is useful both as duration and estimated completion.
            time_val = charging_remaining_time
            if time_val is None:
                time_val = remaining_time_to_full_charge

            if time_val is not None:
                try:
                    remaining_td = timedelta(minutes=int(float(time_val)))
                    self._set_custom_duration(charging, 'remaining_time', remaining_td)
                    if charging.estimated_date_reached is not None:
                        estimated_completion = datetime.now(tz=timezone.utc) + remaining_td
                        charging.estimated_date_reached._set_value(
                            value=estimated_completion
                        )  # pylint: disable=protected-access
                except (TypeError, ValueError):
                    LOG_API.debug("Invalid charging remaining time for %s: %s", vin, time_val)

            self._set_custom_string(
                charging,
                'remaining_time_last_update',
                remaining_time_last_update
            )

            if charging_instantaneous_power is not None and charging.power is not None:
                charging.power._set_value(
                    value=float(charging_instantaneous_power)
                )  # pylint: disable=protected-access

        # Timestamp / measurement time
        if battery_timestamp:
            try:
                measured = robust_time_parse(battery_timestamp)
                if isinstance(vehicle.charging, RenaultCharging) and vehicle.charging.state is not None:
                    vehicle.charging.state._set_value(
                        value=vehicle.charging.state.value,
                        measured=measured
                    )  # pylint: disable=protected-access
            except ValueError as err:
                LOG_API.debug("Could not parse battery timestamp for %s: %s", vin, err)

        log_extra_keys(LOG, 'battery-status.attributes', attributes, {
            'batteryLevel', 'batteryAutonomy', 'batteryAvailableEnergy',
            'batteryTemperature', 'V2L_SystemStatusDisplay',
            'chargingStatus', 'plugStatus', 'chargingRemainingTime',
            'chargingRemainingTimeLastUpdateDateTime',
            'remainingTime', 'chargingInstantaneousPower',
            'timestamp', 'chargeEnergy', 'chargePower',
        })


    def _fetch_hvac_status(self, account_id: str, vin: str, vehicle: RenaultVehicle) -> None:
        """Fetch HVAC status for a vehicle."""
        try:
            data, used_account_id = self._kamereon_get_vehicle_data(
                account_id, vin, 'hvac-status', version=1
            )
            LOG_API.debug(
                "HVAC status for %s via account %s: %s",
                vin,
                used_account_id,
                json.dumps(data, indent=2)
            )
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code in (404, 501):
                LOG_API.debug("HVAC status endpoint not available for %s", vin)
                return
            LOG.warning("Failed to fetch HVAC status for %s: %s", vin, err)
            return
        except Exception as err:  # pylint: disable=broad-except
            LOG.warning("Failed to fetch HVAC status for %s: %s", vin, err)
            return

        attributes = data.get('data', {}).get('attributes', {})
        if not attributes:
            return

        hvac_status_str = attributes.get('hvacStatus')
        external_temp = attributes.get('externalTemperature')
        soc_threshold = attributes.get('socThreshold')
        last_update = attributes.get('lastUpdateTime')
        next_hvac_start = attributes.get('nextHvacStartDate')

        if hvac_status_str and isinstance(vehicle.climatization, RenaultClimatization):
            try:
                renault_hvac_state = RenaultClimatization.RenaultClimatizationState(hvac_status_str)
                generic_state = mapping_renault_climatization_state.get(
                    renault_hvac_state,
                    Climatization.ClimatizationState.UNKNOWN
                )
                if vehicle.climatization.state is not None:
                    measured = None
                    if last_update:
                        try:
                            measured = robust_time_parse(last_update)
                        except ValueError:
                            measured = None
                    vehicle.climatization.state._set_value(
                        value=generic_state,
                        measured=measured
                    )  # pylint: disable=protected-access
            except ValueError:
                LOG_API.debug("Unknown HVAC state for %s: %s", vin, hvac_status_str)

        if external_temp is not None and vehicle.outside_temperature is not None:
            vehicle.outside_temperature._set_value(
                value=float(external_temp),
                unit=Temperature.C
            )  # pylint: disable=protected-access

        # Spring response fields exposed as explicit MQTT topics.
        if vehicle.climatization is not None:
            self._set_custom_string(vehicle.climatization, 'soc_threshold', soc_threshold)
            self._set_custom_string(vehicle.climatization, 'last_update', last_update)
            self._set_custom_string(vehicle.climatization, 'next_hvac_start', next_hvac_start)

        log_extra_keys(LOG, 'hvac-status.attributes', attributes, {
            'hvacStatus', 'externalTemperature', 'socThreshold',
            'lastUpdateTime', 'nextHvacStartDate',
        })


    def _fetch_location(self, account_id: str, vin: str, vehicle: RenaultVehicle) -> None:
        """Fetch location data for a vehicle."""
        try:
            data, used_account_id = self._kamereon_get_vehicle_data(
                account_id, vin, 'location', version=1
            )
            LOG_API.debug(
                "Location data for %s via account %s: %s",
                vin,
                used_account_id,
                json.dumps(data, indent=2)
            )
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code in (404, 501):
                LOG_API.debug("Location endpoint not available for %s", vin)
                return
            LOG.warning("Failed to fetch location data for %s: %s", vin, err)
            return
        except Exception as err:  # pylint: disable=broad-except
            LOG.warning("Failed to fetch location data for %s: %s", vin, err)
            return

        attributes = data.get('data', {}).get('attributes', {})
        if not attributes:
            return

        latitude = attributes.get('gpsLatitude')
        longitude = attributes.get('gpsLongitude')
        last_updated_str = attributes.get('lastUpdateTime')

        if latitude is not None and longitude is not None and vehicle.position is not None:
            last_updated_ts = None
            if last_updated_str:
                try:
                    last_updated_ts = robust_time_parse(last_updated_str)
                except ValueError as err:
                    LOG_API.debug("Could not parse location timestamp for %s: %s", vin, err)

            if vehicle.position.latitude is not None:
                vehicle.position.latitude._set_value(
                    value=float(latitude),
                    measured=last_updated_ts
                )  # pylint: disable=protected-access

            if vehicle.position.longitude is not None:
                vehicle.position.longitude._set_value(
                    value=float(longitude),
                    measured=last_updated_ts
                )  # pylint: disable=protected-access

            self._set_custom_string(vehicle.position, 'last_update', last_updated_str)

        log_extra_keys(LOG, 'location.attributes', attributes, {
            'gpsLatitude', 'gpsLongitude', 'lastUpdateTime',
        })

    def shutdown(self) -> None:
        """Shut down the connector gracefully."""
        self._stop_event.set()
        if self._background_thread is not None:
            self._background_thread.join(timeout=10)
        self._persist_tokens()

    def get_version(self) -> str:
        """Return the connector version."""
        return __version__

    def get_type(self) -> str:
        """Return the connector type identifier."""
        return "renaultdacia"

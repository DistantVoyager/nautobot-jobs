"""Seed a Nautobot instance with the demo.nautobot.com dataset -- single-file job.

Drop this one file into a Git repository (or JOBS_ROOT) and Nautobot will register a
single job, "Seed Demo Data (NDG designs)", which runs the Design Builder jobs from
networktocode-llc/nautobot-data-generation (NDG) in dependency order.

Upstream, that ordering only exists in NDG's `render_*_designs` management commands,
which need shell access. This job reproduces them so it works on a managed instance
such as Nautobot Cloud.

Prerequisites
-------------
* `nautobot-design-builder` >= 3.1.0 installed and enabled.
* The NDG repo synced as a Nautobot Git repository providing "jobs", so its design
  jobs are registered. NDG is private, so that repository needs credentials.
* `nautobot-golden-config` and `nautobot-dns-models` only for the optional
  NetworkToCode stages.

Each design runs as its own JobResult via `JobResult.enqueue_job(..., synchronous=True)`,
which keeps Design Builder's per-job bookkeeping intact while still running everything
in order inside this job's worker process.

The site tables at the bottom are copied from NDG's
`nautobot_gizmo_designs/management/commands/__init__.py`. That package sits outside
NDG's `jobs/` directory, so it is not importable when only `jobs/` is synced as a Git
job source -- hence the local copy. This job prefers the upstream package whenever it
*is* importable, so the data stays current; re-copy these tables if the upstream site
list changes.
"""

from dataclasses import dataclass

from nautobot.apps.jobs import BooleanVar, IntegerVar, Job, register_jobs
from nautobot.dcim.models import Location
from nautobot.extras.choices import JobResultStatusChoices
from nautobot.extras.models import GitRepository, Job as JobModel, JobResult

name = "Demo Data"


# Sites the NetworkToCode stages build, mirroring render_networktocode_designs.
NTC_SITES = [
    {"site_code": "nyc", "backbone_platform": "Juniper Junos", "switch_platform": "Arista EOS"},
    {"site_code": "jcy", "backbone_platform": "Cisco IOS", "switch_platform": "Cisco NX-OS"},
    {"site_code": "wee", "backbone_platform": "Juniper Junos", "switch_platform": "Cisco NX-OS"},
]

# Modules holding the NDG design jobs, relative to wherever NDG is installed.
#
# A job's `module_name` depends on how NDG reached the instance: jobs loaded from
# JOBS_ROOT keep their bare module path (`jobs.designs.branch`), while jobs synced
# from a Git repository are namespaced under the repository slug
# (`nautobot-data-generation.jobs.designs.branch`). The prefix is therefore
# discovered at runtime -- see `_resolve_module_prefix`.
NDG_SUBMODULES = [
    "jobs.designs.branch",
    "jobs.designs.backbone",
    "jobs.designs.cloud",
    "jobs.designs.datacenter",
    "jobs.designs.networktocode",
    "jobs.designs.vpn",
]

# A design that is guaranteed to exist, used to locate the NDG module prefix.
NDG_SENTINEL = ("jobs.designs.branch", "CreateBase")


@dataclass
class Site:
    """One branch or data-center site definition."""

    site_name: str
    region_name: str
    country_name: str
    site_facility: str
    site_status: str
    site_latitude: float
    site_longitude: float
    site_address: str
    site_access_switch_count: int = 0
    has_experimental_sdwan_deployment: bool = False
    switch_platform: str = ""


# (name, region, country, facility, status, lat, long, access_switches, sdwan, address)
_BRANCH_ROWS = [
    ('NYC01', 'AMER', 'USA', 'One World Trade Center', 'Active', 40.713, -74.01319, 6, True, 'One World Trade Center, 285, Fulton Street, Financial District, Manhattan, New York County, New York, 10048, United States'),
    ('LON01', 'EMEA', 'United Kingdom', 'The Shard', 'Active', 51.50434, -0.08645, 5, False, 'The Shard, 32, London Bridge Street, London Bridge Quarter, The Borough, London Borough of Southwark, London, Greater London, England, SE1 9SG, United Kingdom'),
    ('SYD01', 'APAC', 'Australia', 'Chifley Tower', 'Active', -33.86595, 151.21167, 3, False, 'Chifley Tower, Hunter Street, Quay Quarter, Sydney, Sydney CBD, Sydney, Council of the City of Sydney, New South Wales, 2000, Australia'),
    ('LAX01', 'AMER', 'USA', 'US Bank Tower', 'Active', 34.05106, -118.25442, 4, False, 'U.S. Bank Tower, 633, Bunker Hill Steps, Bunker Hill, Downtown, Los Angeles, Los Angeles County, California, 90071, United States'),
    ('AMS01', 'EMEA', 'Netherlands', 'Rembrandt Tower', 'Active', 52.34513, 4.91706, 2, True, 'Rembrandt Tower, Weesperzijde, De Omval, Oost, Amsterdam, North Holland, Netherlands, 1096 BC, Netherlands'),
    ('SIN01', 'APAC', 'Singapore', 'Marina Bay Financial Centre', 'Active', 1.27954, 103.85437, 2, False, 'Marina Bay Financial Centre, Straits View, Downtown Central, Downtown Core, Central, Singapore, 018984, Singapore'),
    ('PAR01', 'EMEA', 'France', 'Tour Montparnasse', 'Active', 48.84211, 2.32197, 4, False, 'Montparnasse Tower, Rue du Départ, Quartier Necker, 15th Arrondissement, Paris, Ile-de-France, Metropolitan France, 75015, France'),
    ('WAW01', 'EMEA', 'Poland', 'Warsaw Spire', 'Active', 52.23218, 20.98409, 7, False, 'Warsaw Spire, 1, Plac Europejski, Mirów, Wola, Warsaw, Masovian Voivodeship, 00-844, Poland'),
    ('PRG01', 'EMEA', 'Czech Republic', 'City Tower', 'Active', 50.05035, 14.43618, 5, False, 'City Tower, Hvězdova, Pankrác, Nusle, Prague, obvod Praha 4, Capital City of Prague, Prague, 140 63, Czechia'),
    ('BER01', 'EMEA', 'Germany', 'Potsdamer Platz', 'Active', 52.5098, 13.37559, 5, False, 'Potsdamer Platz, Tiergarten, Mitte, Berlin, 10785, Germany'),
    ('MEL01', 'APAC', 'Australia', 'Eureka Tower', 'Active', -37.82175, 144.96465, 2, False, 'Eureka Tower, 7, Riverside Quay, Southbank, Melbourne, City of Melbourne, Victoria, 3006, Australia'),
    ('TOR01', 'AMER', 'Canada', 'First Canadian Place', 'Active', 43.64877, -79.38169, 7, False, 'First Canadian Place, 100, King Street West, Financial District, Spadina—Fort York, Old Toronto, Toronto, Golden Horseshoe, Ontario, M5X 1C7, Canada'),
    ('MEX01', 'AMER', 'Mexico', 'Torre Mayor', 'Active', 19.42435, -99.1753, 3, False, 'Torre Mayor, 505, Avenida Paseo de la Reforma, Cuauhtémoc, Mexico City, Cuauhtémoc, Mexico City, 06500, Mexico'),
    ('HOU01', 'AMER', 'USA', 'JPMorgan Chase Tower', 'Active', 29.76032, -95.364, 7, False, 'JPMorgan Chase Tower, 600, Travis Street, Downtown, Houston, Harris County, Texas, 77002, United States'),
    ('CHI01', 'AMER', 'USA', 'Willis Tower', 'Active', 41.87874, -87.63596, 6, False, "Willis Tower, 233, South Wacker Drive, Printer's Row, Loop, Chicago, Cook County, Illinois, 60606, United States"),
    ('ROM01', 'EMEA', 'Italy', 'Piazza Venezia', 'Active', 41.89615, 12.48241, 5, False, 'Piazza Venezia, Pigna, Municipio Roma I, Rome, Roma Capitale, Lazio, Italy'),
    ('PHX01', 'AMER', 'USA', 'Renaissance Square', 'Active', 33.4488, -112.07466, 3, False, 'Renaissance Square, 10032, Phoenix, Maricopa County, Arizona, 85004, United States'),
    ('SAO01', 'AMER', 'Brazil', 'Paulista Avenue', 'Active', -23.5505, -46.6333, 2, False, 'Paulista Avenue, Av. Paulista'),
    ('SAN01', 'AMER', 'USA', 'One America Plaza', 'Active', 32.71637, -117.16876, 7, False, "One America Plaza, 600, West Broadway, Core-Columbia, Banker's Hill, San Diego, San Diego County, California, 92101, United States"),
    ('DAL01', 'AMER', 'USA', 'Bank of America Plaza', 'Active', 32.78007, -96.80389, 3, False, 'Bank of America Plaza, 901, Main Street, West End Historic District, Downtown PID, Dallas, Dallas County, Texas, 75202, United States'),
    ('VAN01', 'AMER', 'Canada', 'Park Place', 'Active', 49.28508, -123.11917, 6, False, 'Park Place, 666, Burrard Street, Coal Harbour, Downtown, Vancouver, Metro Vancouver Regional District, British Columbia, V6C, Canada'),
    ('MAD01', 'EMEA', 'Spain', 'Torre de Cristal', 'Active', 40.47817, -3.68752, 5, False, 'Torre de Cristal, 259 C, Paseo de la Castellana, La Paz, Fuencarral-El Pardo, Madrid, Community of Madrid, 28046, Spain'),
    ('BUE01', 'AMER', 'Argentina', 'Torre Alvear', 'Active', -34.59687, -58.99375, 4, False, 'La Torre, Alvear, La Fraternidad, Partido de General Rodríguez, Buenos Aires, B1748, Argentina'),
    ('SFO01', 'AMER', 'USA', 'Salesforce Tower', 'Active', 37.78977, -122.39693, 3, False, 'Salesforce Tower, 415, Mission Street, Transbay, South of Market, San Francisco, California, 94105, United States'),
    ('BOS01', 'AMER', 'USA', 'Hancock Tower', 'Active', 42.3601, -71.0589, 7, False, 'Hancock Tower, 200 Clarendon St'),
    ('RIO01', 'AMER', 'Brazil', 'Edifício Central', 'Active', -22.90183, -43.17938, 5, False, 'Edifício Central, 417, Avenida Presidente Vargas, Saara, Centro, Rio de Janeiro, Região Geográfica Imediata do Rio de Janeiro, Região Metropolitana do Rio de Janeiro, Região Geográfica Intermediária do Rio de Janeiro, Rio de Janeiro, Southeast Region, 20090-004, Brazil'),
    ('TOK01', 'APAC', 'Japan', 'Roppongi Hills Mori Tower', 'Active', 35.66046, 139.72928, 6, False, 'Roppongi Hills Mori Tower, Mafu Tunnel, Roppongi 6-chome, Roppongi, Minato, Tokyo, 106-6188, Japan'),
    ('DEN01', 'AMER', 'USA', 'Republic Plaza', 'Active', 39.74326, -104.98907, 6, False, 'Republic Plaza, Central Business District, Denver, Colorado, 80274, United States'),
    ('MIA01', 'AMER', 'USA', 'Four Seasons Hotel', 'Active', 25.75896, -80.19211, 4, False, 'Four Seasons Hotel Miami, Southeast 14th Terrace, Miami, Miami-Dade County, Florida, 33131, United States'),
    ('DUB01', 'EMEA', 'Ireland', 'The Convention Centre Dublin', 'Active', 53.34825, -6.23951, 4, False, 'The Convention Centre Dublin, 1, North Wall Quay, North Wall, North Dock B ED, Dublin, County Dublin, Leinster, D01 T1W6, Ireland'),
    ('NYC02', 'AMER', 'USA', 'Empire State Building', 'Active', 40.74844, -73.98566, 4, False, 'Empire State Building, 350, 5th Avenue, Manhattan Community Board 5, Manhattan, New York County, New York, 10118, United States'),
    ('ATL01', 'AMER', 'USA', 'Bank of America Plaza', 'Active', 33.77085, -84.38614, 7, False, 'Bank of America Plaza, 600, Peachtree Street Northeast, Old Fourth Ward, Atlanta, Fulton County, Georgia, 30308, United States'),
    ('ANY01', 'AMER', 'USA', 'Empire State Plaza', 'Active', 42.65092, -73.75978, 2, False, 'Empire State Plaza, Hudson Avenue, Lark Street, City of Albany, Albany County, New York, 12210, United States'),
    ('DET01', 'AMER', 'USA', 'Renaissance Center', 'Active', 42.32892, -83.03966, 5, False, 'Renaissance Center, 100, Renaissance Center, Detroit, Wayne County, Michigan, 48243, United States'),
    ('PHI01', 'AMER', 'USA', 'Comcast Technology Center', 'Active', 39.95498, -75.1704, 3, False, 'Comcast Technology Center, 1800, Arch Street, Center City, Philadelphia, Philadelphia County, Pennsylvania, 19103, United States'),
    ('SCL01', 'AMER', 'Chile', 'Costanera Center', 'Active', -33.41704, -70.60578, 4, False, 'Costanera Center, Nueva Tobalaba, Sanhattan, Providencia, Provincia de Santiago, Santiago Metropolitan Region, 7550099, Chile'),
    ('LON02', 'EMEA', 'United Kingdom', '20 Fenchurch Street', 'Active', 51.51127, -0.08354, 6, False, 'The Walkie Talkie, 20, Fenchurch Street, Leadenhall Market, City of London, Greater London, England, EC3M 7HJ, United Kingdom'),
    ('CLT01', 'AMER', 'USA', 'Bank of America Corporate Center', 'Active', 35.22731, -80.84223, 3, False, 'Bank of America Corporate Center, 100, North Tryon Street, Uptown, Charlotte, Mecklenburg County, North Carolina, 28202, United States'),
    ('CPH01', 'EMEA', 'Denmark', 'Copenhagen Towers', 'Active', 55.62745, 12.57706, 7, False, 'Copenhagen Towers, Øresundsmotorvejen, Våren, Copenhagen, Copenhagen Municipality, Capital Region of Denmark, 1561, Denmark'),
    ('MON01', 'AMER', 'Canada', '1000 de La Gauchetière', 'Active', 45.4982, -73.56625, 5, False, '1000 de la Gauchetière, Rue Saint-Antoine Ouest, Vieux-Montréal, Ville-Marie, Montreal, Urban agglomeration of Montreal, Montreal (administrative region), Quebec, H3B 4W5, Canada'),
    ('LIM01', 'AMER', 'Peru', 'Torre Begonias', 'Active', -12.0921, -77.02396, 4, False, 'Torre Begonias, Calle Las Begonias, Centro financiero de San Isidro, San Isidro, Province of Lima, Lima Metropolitan Area, Lima, 15046, Peru'),
    ('STO01', 'EMEA', 'Sweden', 'Kista Science Tower', 'Active', 59.40102, 17.94695, 3, False, 'Kista Science Tower, Hanstavägen, Kista, Järva stadsdelsområde, Stockholm, Stockholm Municipality, Stockholm County, 164 21, Sweden'),
    ('AUS01', 'AMER', 'USA', 'The Austonian', 'Active', 30.26479, -97.74449, 4, False, 'The Austonian, West 2nd Street, Warehouse District, Austin, Travis County, Texas, 78701, United States'),
    ('NOL01', 'AMER', 'USA', 'Place St. Charles', 'Active', 29.95202, -90.07036, 2, False, 'Place St. Charles, Gravier Street, Central Business District, French Quarter, New Orleans, Orleans Parish, Louisiana, 70170, United States'),
    ('OSL01', 'EMEA', 'Norway', 'Barcode Project', 'Active', 59.9139, 10.7522, 5, False, 'Barcode Project, Dronning Eufemias gate 16'),
    ('SEA01', 'AMER', 'USA', 'Columbia Center', 'Active', 47.60454, -122.33072, 5, False, 'Columbia Center, 701, 5th Avenue, West Edge, First Hill, Seattle, King County, Washington, 98104, United States'),
    ('BRU01', 'EMEA', 'Belgium', 'Tour du Midi', 'Active', 50.83815, 4.33772, 3, False, "South Tower, Esplanade de l'Europe - Europaesplanade, Saint-Gilles - Sint-Gillis, Brussels-Capital, 1060, Belgium"),
    ('CIN01', 'AMER', 'USA', 'Great American Tower', 'Active', 39.09978, -84.50709, 3, False, 'Great American Tower at Queen City Square, East 3rd Street, Lytle Park Historic District, Central Business District, Cincinnati, Hamilton County, Ohio, 45202, United States'),
    ('CLE01', 'AMER', 'USA', 'Key Tower', 'Active', 41.50106, -81.69386, 4, False, 'Key Tower, 127, East Roadway, Downtown Cleveland, Cleveland, Cuyahoga County, Ohio, 44115, United States'),
    ('LIS01', 'EMEA', 'Portugal', 'Torre Vasco da Gama', 'Active', 38.77476, -9.09147, 4, False, 'Vasco da Gama Tower, Cais das Naus, Parque das Nações, Lisbon, 1990-173, Portugal'),
    ('ANG01', 'AMER', 'USA', 'Angel Stadium', 'Active', 33.80026, -117.88174, 2, False, 'Angel Stadium of Anaheim, 2000, East Gene Autry Way, Anaheim Resort District, Anaheim, Orange County, California, 92806, United States'),
    ('SLC01', 'AMER', 'USA', 'Busch Stadium', 'Active', 38.62255, -90.19392, 7, False, 'Busch Stadium, 700, Clark Avenue, Downtown, St. Louis, Missouri, 63101, United States'),
    ('AZD01', 'AMER', 'USA', 'Chase Field', 'Active', 33.44549, -112.06669, 5, False, 'Chase Field, East Jefferson Street, Phoenix, Maricopa County, Arizona, 85004, United States'),
    ('NYM01', 'AMER', 'USA', 'Citi Field', 'Active', 40.75728, -73.84588, 4, False, 'Citi Field, 41, Seaver Way, Queens, Queens County, New York, 11368, United States'),
    ('PHP01', 'AMER', 'USA', 'Citizens Bank Park', 'Active', 39.90589, -75.16541, 5, False, 'Citizens Bank Park, Hartranft Street, South Philadelphia Sports Complex, South Philadelphia, Philadelphia, Philadelphia County, Pennsylvania, 19148, United States'),
    ('DTT01', 'AMER', 'USA', 'Comerica Park', 'Active', 42.339, -83.04886, 7, False, 'Comerica Park, Detroit Athletic Club Pavillion, Greektown, Detroit, Wayne County, Michigan, 48226, United States'),
    ('COR01', 'AMER', 'USA', 'Coors Field', 'Active', 39.75603, -104.99293, 4, False, 'Coors Field, 2001, Blake Street, Five Points, Denver, Colorado, 80205, United States'),
    ('LAD01', 'AMER', 'USA', 'Dodger Stadium', 'Active', 34.07363, -118.23898, 6, False, 'Dodger Stadium, 1000, Vin Scully Avenue, Chinatown, Los Angeles, Los Angeles County, California, 90012, United States'),
    ('BRS01', 'AMER', 'USA', 'Fenway Park', 'Active', 42.34646, -71.0971, 4, False, 'Fenway Park, 4, Jersey Street, Audubon Circle, Fenway-Kenmore, Boston, Suffolk County, Massachusetts, 02115, United States'),
    ('TXR01', 'AMER', 'USA', 'Globe Life Field', 'Active', 32.74761, -97.08411, 7, False, 'Globe Life Field, 734, Stadium Drive, Arlington, Tarrant County, Texas, 76011, United States'),
    ('CIR01', 'AMER', 'USA', 'Great American Ball Park', 'Active', 39.0974, -84.50662, 7, False, 'Great American Ball Park, Ohio River Trail, Lytle Park Historic District, Central Business District, Cincinnati, Hamilton County, Ohio, 45202, United States'),
    ('CWS01', 'AMER', 'USA', 'Guaranteed Rate Field', 'Active', 41.82969, -87.63379, 4, False, 'Guaranteed Rate Field, 333, West 35th Street, Armour Square, Chicago, Cook County, Illinois, 60616, United States'),
    ('KCR01', 'AMER', 'USA', 'Kauffman Stadium', 'Active', 39.05147, -94.4814, 4, False, 'Kauffman Stadium, 1, Royal Way, Kansas City, Jackson County, Missouri, 64129, United States'),
    ('MIM01', 'AMER', 'USA', 'LoanDepot Park', 'Active', 25.77811, -80.21955, 6, False, 'loanDepot Park, Northwest 6th Street, Miami, Miami-Dade County, Florida, 33128, United States'),
    ('HAS01', 'AMER', 'USA', 'Minute Maid Park', 'Active', 29.75723, -95.35523, 5, False, 'Daikin Park, 501, Crawford Street, Downtown, Houston, Harris County, Texas, 77004, United States'),
    ('WNA01', 'AMER', 'USA', 'Nationals Park', 'Active', 38.87274, -77.00839, 4, False, 'Nationals Park, 1500, South Capitol Street Southeast, Ward 8, Washington, District of Columbia, 20590, United States'),
    ('OAT01', 'AMER', 'USA', 'Oakland Coliseum', 'Active', 37.75168, -122.19937, 5, False, 'Oakland-Alameda County Coliseum, 7000, Coliseum Way, Oakland-Alameda County Coliseum Complex, Oakland, Alameda County, California, 94621, United States'),
    ('SFG01', 'AMER', 'USA', 'Oracle Park', 'Active', 37.77861, -122.39027, 4, False, 'Oracle Park, 24, Willie Mays Plaza, South Beach, South of Market, San Francisco, California, 94107, United States'),
    ('BOR01', 'AMER', 'USA', 'Oriole Park at Camden Yards', 'Active', 39.28374, -76.62161, 7, False, 'Oriole Park at Camden Yards, Eutaw Street, Baltimore, Maryland, 21201, United States'),
    ('SDP01', 'AMER', 'USA', 'Petco Park', 'Active', 32.70719, -117.15688, 5, False, 'Petco Park, 100, Park Boulevard, East Village, San Diego, San Diego County, California, 92101, United States'),
    ('PIP01', 'AMER', 'USA', 'PNC Park', 'Active', 40.44693, -80.00561, 4, False, 'PNC Park, Mazeroski Way, North Shore, Pittsburgh, Allegheny County, Pennsylvania, 15222, United States'),
    ('CLI01', 'AMER', 'USA', 'Progressive Field', 'Active', 41.49609, -81.68513, 4, False, 'Progressive Field, 2401, Ontario Street, Downtown Cleveland, Cleveland, Cuyahoga County, Ohio, 44115, United States'),
    ('TBJ01', 'AMER', 'Canada', 'Rogers Centre', 'Active', 43.64166, -79.3892, 6, False, 'Rogers Centre, 1, Blue Jays Way, Entertainment District, Spadina—Fort York, Old Toronto, Toronto, Golden Horseshoe, Ontario, M5V 1J1, Canada'),
    ('SEM01', 'AMER', 'USA', 'T-Mobile Park', 'Active', 47.59149, -122.3321, 7, False, 'T-Mobile Park, South Royal Brougham Way, Yesler Terrace, Seattle, King County, Washington, 98104, United States'),
    ('MIT01', 'AMER', 'USA', 'Target Field', 'Active', 44.98168, -93.27786, 4, False, 'Target Field, 1, Twins Way, Minneapolis, Hennepin County, Minnesota, 55403, United States'),
    ('TBR01', 'AMER', 'USA', 'Tropicana Field', 'Active', 27.76806, -82.65328, 5, False, 'Tropicana Field, Pinellas Trail, Edge District, Saint Petersburg, Pinellas County, Florida, 33701, United States'),
    ('ATB01', 'AMER', 'USA', 'Truist Park', 'Active', 33.89071, -84.46853, 7, False, 'Truist Park, 755, Battery Avenue Southeast, The Battery Atlanta, Atlanta, Cobb County, Georgia, 30339, United States'),
    ('CRC01', 'AMER', 'USA', 'Wrigley Field. 1060 W Addison St', 'Active', 41.94818, -87.65556, 5, False, 'Wrigley Field, 1060, West Addison Street, Wrigleyville, Lake View, Chicago, Lake View Township, Cook County, Illinois, 60613, United States'),
    ('NYY01', 'AMER', 'USA', 'Yankee Stadium', 'Active', 40.82958, -73.92652, 6, False, 'Yankee Stadium, 1, East 161st Street, The Bronx, Bronx County, New York, 10451, United States'),
    ('HEL01', 'EMEA', 'Finland', 'Sanoma House', 'Active', 60.1699, 24.9384, 6, False, 'Sanoma House, Töölönlahdenkatu 2'),
    ('KAN01', 'AMER', 'USA', 'One Kansas City Place', 'Active', 39.09964, -94.58379, 6, False, 'One Kansas City Place, 1200, Main Street, Central Downtown, Power & Light District, Downtown Kansas City, Kansas City, Jackson County, Missouri, 64105, United States'),
    ('REN01', 'AMER', 'USA', '407 N Virginia St', 'Active', 39.53037, -119.81577, 4, False, 'Silver Legacy Reno, 407, North Virginia Street, Reno, Washoe County, Nevada, 89503, United States'),
    ('WAW02', 'EMEA', 'Poland', 'Rondo 1', 'Active', 52.23285, 20.99951, 3, False, 'Rondo 1, 1, Rondo ONZ, Za Żelazną Bramą, Śródmieście Północne, Midtown, Warsaw, Masovian Voivodeship, 00-124, Poland'),
    ('STL01', 'AMER', 'USA', 'One Metropolitan Square', 'Active', 38.627, -90.18983, 3, False, 'One Metropolitan Square, North 6th Street, Downtown, St. Louis, Missouri, 63106, United States'),
    ('BNE01', 'APAC', 'Australia', 'Riparian Plaza', 'Active', -27.46823, 153.03041, 2, False, 'Riparian Plaza, 71, Eagle Street, Golden Triangle, Brisbane City, Greater Brisbane, Queensland, 4000, Australia'),
    ('CBO01', 'AMER', 'Mexico', 'Plaza Bonita', 'Active', 22.88621, -109.9114, 4, False, 'Plaza Bonita, Boulevard Paseo de Marina, City Centre, Cabo San Lucas, Los Cabos Municipality, Baja California Sur, 23450, Mexico'),
    ('EDI01', 'EMEA', 'United Kingdom', 'Edinburgh One', 'Active', 55.94598, -3.20747, 4, False, 'Edinburgh One, Morrison Street, The Exchange, Tollcross, City of Edinburgh, Scotland, EH3 8EX, United Kingdom'),
    ('HNL01', 'AMER', 'USA', 'First Hawaiian Center', 'Active', 21.45845, -158.01489, 5, False, 'First Hawaiian Bank, Kuahelani Avenue, Mililani Shopping Center, Mililani Town, Honolulu County, Hawaii, 96789, United States'),
    ('GLA01', 'EMEA', 'United Kingdom', 'The Pinnacle', 'Active', 55.86191, -4.26527, 2, False, 'The Pinnacle Building, 160, Bothwell Street, Blythswood Holm, Glasgow, Glasgow City, Scotland, G2 7EA, United Kingdom'),
    ('MIL01', 'EMEA', 'Italy', 'Torre Unicredit', 'Active', 45.48386, 9.18991, 3, False, 'Unicredit Tower, 1785_0, GARIBALDI REPUBBLICA, Municipio 9, Milan, Lombardy, 20100, Italy'),
    ('PER01', 'APAC', 'Australia', 'Central Park Tower', 'Active', -31.9505, 115.8605, 7, False, 'Central Park Tower, 152-158 St Georges Terrace'),
    ('BUD01', 'EMEA', 'Hungary', 'Budapest One', 'Active', 47.4648, 19.01537, 5, False, 'Budapest ONE Business Park, 2/A, Balatoni út, Őrmezői lakótelep, Őrmező, 11th district, Budapest, Central Hungary, 1112, Hungary'),
    ('POR01', 'AMER', 'USA', 'US Bancorp Tower', 'Active', 45.52235, -122.67623, 3, False, 'US Bancorp Tower, Southwest Pine Street, Downtown, Portland, Multnomah County, Oregon, 97240, United States'),
    ('SJC01', 'AMER', 'USA', 'Fairmont Plaza', 'Active', 37.33387, -121.88929, 6, False, 'Fairmont Plaza, Downtown Historic District, San Jose, Santa Clara County, California, United States'),
    ('BIR01', 'EMEA', 'United Kingdom', 'Colmore Gate', 'Active', 52.48243, -1.89708, 4, False, 'Colmore Gate, Colmore Row, Jewellery Quarter, Park Central, Birmingham, West Midlands, England, B3 2QA, United Kingdom'),
    ('AMS02', 'EMEA', 'Netherlands', 'The Edge', 'Active', 52.33716, 4.86202, 6, False, 'The Edge, Hildegard von Bingenstraat, VU-kwartier, Zuidas, Zuid, Amsterdam, North Holland, Netherlands, 1081 LB, Netherlands'),
    ('ATH01', 'EMEA', 'Greece', 'Athens Tower', 'Active', 37.98463, 23.76075, 5, False, 'Athens Tower 1, Σινώπης, Κουντουριώτικα, Ambelokipoi, 7th District of Athens, Athens, Municipality of Athens, Regional Unit of Central Athens, Attica, 115 27, Greece'),
    ('SLC01', 'AMER', 'USA', '111 Main', 'Active', 40.76682, -111.8906, 4, False, '111 Main, 111 S, Main Street, Salt Lake City, Salt Lake County, Utah, 84101, United States'),
    ('OMA01', 'AMER', 'USA', 'First National Tower', 'Active', 41.25919, -95.93787, 3, False, 'First National Tower, 1601 Dodge St'),
    ('BCN01', 'EMEA', 'Spain', 'Torre Glòries', 'Active', 41.40352, 2.18952, 4, False, 'Glòries Tower, Carrer de Badajoz, el Parc i la Llacuna del Poblenou, Sant Martí, Barcelona, Barcelonès, Barcelona, Catalonia, 08018, Spain'),
    ('WCH01', 'AMER', 'USA', 'Epic Center', 'Active', 37.69017, -97.33872, 5, False, 'Epic Center, 301, North Main Street, Wichita, Sedgwick County, Kansas, 67202, United States'),
    ('MUN01', 'EMEA', 'Germany', 'Highlight Towers', 'Active', 48.1766, 11.59273, 5, False, 'Highlight Towers, 6, Mies-van-der-Rohe-Straße, Parkstadt Schwabing, Alte Heide - Hirschau, Schwabing-Freimann, Munich, Bavaria, 80807, Germany'),
    ('ZRH01', 'EMEA', 'Switzerland', 'Prime Tower', 'Active', 47.38609, 8.51727, 7, False, 'Prime Tower, 201, Hardstrasse, Escher Wyss, Industriequartier, Zurich, District Zurich, Zurich, 8010, Switzerland'),
    ('VIE01', 'EMEA', 'Austria', 'DC Tower 1', 'Active', 48.23189, 16.4127, 4, False, 'DC Tower 1, 7, Donau-City-Straße, Donau City, KG Kaisermühlen, Donaustadt, Vienna, 1220, Austria'),
    ('MOB01', 'AMER', 'USA', 'RSA Battle House Tower', 'Active', 30.6954, -88.0399, 6, False, 'RSA Battle House Tower, 11 N Water St'),
    ('SIN02', 'APAC', 'Singapore', 'One Raffles Place', 'Active', 1.2843, 103.85106, 7, False, 'One Raffles Place, 1, Raffles Place, Downtown Core, Central, Singapore, 048616, Singapore'),
    ('NAS01', 'AMER', 'USA', 'AT&T Building', 'Active', 36.16208, -86.77711, 7, False, 'AT&T Building, 333, Commerce Street, Downtown Nashville, Nashville-Davidson, Davidson County, Middle Tennessee, Tennessee, 37201, United States'),
    ('MAN01', 'EMEA', 'United Kingdom', 'Beetham Tower', 'Active', 53.47544, -2.25053, 5, False, 'Beetham Tower, 301-303, Deansgate, St. Johns, City Centre, Manchester, Greater Manchester, England, M3 4LQ, United Kingdom'),
    ('BKK01', 'APAC', 'Thailand', 'King Power Mahanakhon', 'Active', 13.72341, 100.52822, 5, False, 'King Power MahaNakhon, Soi Si Lom 9, Lalai Sap, Si Lom Subdistrict, Bang Rak District, Bangkok, 10500, Thailand'),
    ('LPL01', 'EMEA', 'United Kingdom', 'The Capital', 'Active', 53.40868, -2.99579, 2, False, 'The Capital, Old Hall Street, Pride Quarter, Vauxhall, Liverpool, Liverpool City Region, England, L3 9PP, United Kingdom'),
    ('NYC03', 'AMER', 'USA', 'One Vanderbilt', 'Active', 40.75297, -73.97854, 2, False, 'One Vanderbilt, 1, Vanderbilt Avenue, Manhattan Community Board 5, Manhattan, New York County, New York, 10017, United States'),
    ('HAM01', 'EMEA', 'Germany', 'Elbtower', 'Active', 53.53501, 10.026, 6, False, 'Elbtower, Zweibrückenstraße, Quartier Elbbrücken, HafenCity, Hamburg-Mitte, Hamburg, 20539, Germany'),
    ('SVO01', 'EMEA', 'Russia', 'Federation Tower', 'Active', 55.74968, 37.53753, 4, False, 'Federation Tower complex, 1st Krasnogvardeyskiy Passage, Камушки, Presnensky District, Moscow, Central Federal District, 123317, Russia'),
    ('CHC01', 'APAC', 'New Zealand', 'PwC Centre', 'Active', -43.53334, 172.63183, 5, False, 'The PwC Centre, 60, Cashel Street, Central City, Linwood-Central-Heathcote Community, Christchurch, Christchurch City, Canterbury, 8013, New Zealand'),
    ('LOU01', 'AMER', 'USA', '400 W Market St', 'Active', 38.25493, -85.7572, 4, False, '400 West Market, 400, West Market Street, Louisville, Jefferson County, Kentucky, 40202, United States'),
    ('DUS01', 'EMEA', 'Germany', 'Dreischeibenhaus', 'Active', 51.22794, 6.7829, 3, False, 'Dreischeibenhaus, Stadtmitte, Stadtbezirk 1, Dusseldorf, North Rhine-Westphalia, 40211, Germany'),
    ('MTG01', 'AMER', 'USA', 'RSA Dexter Avenue Building', 'Active', 32.3792, -86.3077, 5, False, 'RSA Dexter Avenue Building, 445 Dexter Ave'),
    ('MOW01', 'EMEA', 'Russia', 'OKO Tower', 'Active', 55.75004, 37.53455, 6, False, 'OKO\xa0– North Tower, 21 с1, 1st Krasnogvardeyskiy Passage, Камушки, Presnensky District, Moscow, Central Federal District, 123317, Russia'),
    ('ICN01', 'APAC', 'South Korea', 'Lotte World Tower', 'Active', 37.51307, 127.10321, 4, False, 'Lotte World Tower & Lotte World Mall, Jamsil 6(yuk)-dong, Songpa-gu, Seoul, 05551, South Korea'),
    ('PEK01', 'APAC', 'China', 'China Zun', 'Active', 39.9115, 116.46024, 3, False, 'CITIC Tower, 10, Guanghua Road, 北京中央商务区, Jianwai Subdistrict, Chaoyang District, Beijing, 100026, China'),
    ('MIN01', 'AMER', 'USA', 'IDS Center', 'Active', 44.9778, -93.265, 6, False, 'IDS Center, 80 S 8th St'),
    ('CHM01', 'APAC', 'Thailand', 'One Nimman', 'Active', 18.80014, 98.96807, 4, False, 'One Nimman, Nimmanhaeminda Road, Chang Phueak, Chiang Mai City Municipality, Pa Daet, Saraphi District, Chiang Mai Province, 50200, Thailand'),
    ('SHA01', 'APAC', 'China', 'Shanghai Tower', 'Active', 31.23564, 121.50125, 7, False, 'Shanghai Tower, 501, Middle Yincheng Road, Lujiazui, Lujiazui Subdistrict, Pudong, Shanghai, 200010, China'),
    ('HKG01', 'APAC', 'China', 'International Commerce Centre', 'Active', 22.30338, 114.16023, 6, False, 'International Commerce Centre, 1, Austin Road West, West Kowloon, Yau Ma Tei, Yau Tsim Mong District, Kowloon, Hong Kong, China'),
    ('KUL01', 'APAC', 'Malaysia', 'Petronas Towers', 'Active', 3.139, 101.7112, 5, False, 'Petronas Towers, Persiaran Petronas, Kuala Lumpur City Centre (KLCC), Bukit Bintang, Kuala Lumpur, 50400, Malaysia'),
    ('AUC01', 'APAC', 'New Zealand', 'PwC Tower', 'Active', -36.84399, 174.76597, 4, False, 'PwC Tower, Little Queen Street, Britomart, City Centre, Auckland, Waitematā, Auckland, 1010, New Zealand'),
    ('HAN01', 'APAC', 'Vietnam', 'Keangnam Hanoi Landmark Tower', 'Active', 21.0285, 105.8542, 6, False, 'Keangnam Hanoi Landmark Tower, Pham Hung Blvd'),
    ('SGN01', 'APAC', 'Vietnam', 'Bitexco Financial Tower', 'Active', 10.77186, 106.70446, 3, False, 'Bitexco Financial Tower, 02, Hai Trieu Street, Ben Nghe Ward, District 1, Ho Chi Minh City, 00084, Vietnam'),
    ('DEL01', 'APAC', 'India', 'DLF Cyber City', 'Active', 28.49792, 77.08869, 7, False, 'DLF Cyber City, Delhi-Gurugram Expressway, Sector 19, Gurgaon, Gurugram, Haryana, 122010, India'),
    ('BOM01', 'APAC', 'India', 'Bandra-Kurla Complex', 'Active', 19.06712, 72.86572, 4, False, 'Bandra Kurla Complex, H/E Ward, Zone 3, Mumbai, Maharashtra, 400098, India'),
    ('HYD01', 'APAC', 'India', 'HITEC City', 'Active', 17.44901, 78.38314, 3, False, 'HITEC City, Cyber Towers - Madhapur Main Road, Vittal Rao Nagar, Ward 104 Kondapur, Greater Hyderabad Municipal Corporation West Zone, Hyderabad, Serilingampalle mandal, Ranga Reddy, Telangana, 500081, India'),
    ('MNL01', 'APAC', 'Philippines', 'Bonifacio Global City', 'Active', 14.55065, 121.04682, 5, False, 'Ascott Bonifacio Global City Manila, 4th Avenue, Fort Bonifacio, Taguig District 2, Taguig, Southern Manila District, Metro Manila, 1635, Philippines'),
    ('CGK01', 'APAC', 'Indonesia', 'Sudirman Central Business District', 'Active', -6.22364, 106.81036, 6, False, 'Sudirman Central Business District Southway, RW 01, Senayan, Kebayoran Baru, South Jakarta, Special capital Region of Jakarta, Java, 12190, Indonesia'),
    ('DPS01', 'APAC', 'Indonesia', 'ITDC Nusa Dua', 'Active', -8.80335, 115.22759, 4, False, 'ITDC BALI, Jalan Pantai Mengiat, Nusa Dua, Benoa, Kuta Selatan, Badung, Bali, Lesser Sunda Islands, 80363, Indonesia'),
    ('SFO02', 'AMER', 'USA', '555 California St', 'Active', 37.79212, -122.40372, 4, False, '555 California Street, 555, California Street, Chinatown, South of Market, San Francisco, California, 94108, United States'),
    ('TPE01', 'APAC', 'Taiwan', 'Taipei 101', 'Active', 25.03384, 121.5645, 7, False, 'Taipei 101, 7, Section 5, Xinyi Road, Xicun Village, Xinyi District, Xinyi Commercial Zone, Taipei, 11049, Taiwan'),
    ('KIX01', 'APAC', 'Japan', 'Umeda Sky Building', 'Active', 34.6937, 135.49053, 5, False, 'Umeda Sky Building, Osaka Itami Line, Oyodonaka 1-chome, Kita Ward, Osaka, Osaka Prefecture, 531-0076, Japan'),
    ('KRA01', 'EMEA', 'Poland', 'K1', 'Active', 50.04694, 19.99715, 5, False, 'Krakow, Lesser Poland Voivodeship, Poland'),
    ('KIE01', 'EMEA', 'Ukraine', 'Gulliver', 'Active', 50.4387, 30.52317, 3, False, 'Gulliver, 1-А, Sportyvna Square, Левашовська Гора, Бессарабка, Klov, Pecherskyi district, Kyiv, 01001, Ukraine'),
    ('MEL02', 'APAC', 'Australia', 'Collins Square', 'Active', -37.82119, 144.95005, 3, False, "Collins Square, 737-747, Collins Street, Batman's Hill, Docklands, Melbourne, City of Melbourne, Victoria, 3008, Australia"),
    ('ZAG01', 'EMEA', 'Croatia', 'Zagreb Tower', 'Active', 45.89951, 15.94803, 6, False, 'Zagreb TV Tower, Staza 44, Gornja Bistra, Općina Bistra, Zagreb County, 10168, Croatia'),
    ('GCS01', 'APAC', 'Australia', 'Q1 Tower', 'Active', -28.0167, 153.4, 3, False, 'Q1 Tower, 9 Hamilton Ave'),
    ('AUC02', 'APAC', 'New Zealand', 'Commercial Bay', 'Active', -36.84379, 174.76645, 2, False, 'Commercial Bay, Little Queen Street, Britomart, City Centre, Auckland, Waitematā, Auckland, 1010, New Zealand'),
    ('RKV01', 'EMEA', 'Iceland', 'Harpa', 'Active', 64.15043, -21.93285, 3, False, 'Harpa, 2, Austurbakki, Miðborg, Reykjavik, Capital Region, 101, Iceland'),
    ('SYD02', 'APAC', 'Australia', 'Barangaroo South', 'Active', -33.86374, 151.20312, 4, False, 'Hickson Rd opp Barangaroo South, Hickson Road, The Hungry Mile, Sydney, Sydney CBD, Sydney, Council of the City of Sydney, New South Wales, 2000, Australia'),
    ('PER02', 'APAC', 'Australia', 'Brookfield Place', 'Active', -31.95464, 115.85496, 5, False, 'Brookfield Place, Perth, City of Perth, Western Australia, 6000, Australia'),
    ('EDM01', 'AMER', 'Canada', 'Manulife Place', 'Active', 53.54265, -113.49439, 3, False, 'Manulife Place, 102 Avenue NW Protected Bike Lane, Downtown, Central Core, Edmonton, Alberta, T5J 0H3, Canada'),
    ('WIN01', 'AMER', 'Canada', '201 Portage', 'Active', 49.89576, -97.13928, 5, False, '201 Portage, 201, Portage Avenue, Portage–Ellice, Winnipeg, Manitoba, R3X 1V3, Canada'),
    ('ADL01', 'APAC', 'Australia', 'Westpac House', 'Active', -34.92485, 138.59876, 6, False, 'RAA Place, 91, King William Street, Adelaide, Adelaide City Council, South Australia, 5000, Australia'),
    ('BNE02', 'APAC', 'Australia', '111 Eagle St', 'Active', -27.4698, 153.03024, 7, False, 'One One One Eagle St, 111, Eagle Street, Golden Triangle, Brisbane City, City of Brisbane, Queensland, 4000, Australia'),
    ('WLG01', 'APAC', 'New Zealand', 'Aon Centre', 'Active', -41.28684, 174.7765, 4, False, 'AON Centre, Willeston Street, Lambton, Wellington Central, Wellington, Wellington City, Wellington, 6140, New Zealand'),
    ('BUC01', 'EMEA', 'Romania', 'SkyTower', 'Active', 44.4268, 26.1025, 7, False, 'SkyTower, Strada Barbu Văcărescu 201'),
    ('BGD01', 'EMEA', 'Serbia', 'Usce Tower', 'Active', 44.81634, 20.43704, 4, False, 'Ušće Tower 1, 6, Булевар Михајла Пупина, MZ Usce, New Belgrade, Belgrade, City of Belgrade, Central Serbia, 11070, Serbia'),
    ('LIE01', 'EMEA', 'Liechtenstein', 'Regierungsgebäude', 'Active', 47.13709, 9.52276, 5, False, 'Regierungsgebäude, 1, Peter-Kaiser-Platz, Ebenholz, Vaduz, Oberland, 9490, Liechtenstein'),
    ('BRN01', 'EMEA', 'Switzerland', 'Bundeshaus', 'Active', 46.9462, 7.44254, 6, False, 'Federal Department of Foreign Affairs, 3, Bundesgasse, Rotes Quartier, Stadtteil I, Bern, Bern-Mittelland administrative district, Bernese Mittelland administrative region, Bern, 3003, Switzerland'),
    ('BOI01', 'AMER', 'USA', '601 W Bannock St', 'Active', 43.61587, -116.20019, 6, False, 'Givens Pursley LLP, 601, West Bannock Street, Downtown, Boise, Ada County, Idaho, 83702, United States'),
    ('CRC02', 'APAC', 'New Zealand', 'HSBC Tower', 'Active', -43.53128, 172.63191, 3, False, 'HSBC Tower, Worcester Boulevard, Central City, Linwood-Central-Heathcote Community, Christchurch, Christchurch City, Canterbury, 8011, New Zealand'),
    ('DAR02', 'APAC', 'Australia', 'NT House', 'Active', -12.46666, 130.84286, 7, False, 'NT Parliament House, Esplanade, Darwin City, Darwin, City of Darwin, Northern Territory, 0800, Australia'),
    ('CNB02', 'APAC', 'Australia', 'Canberra House', 'Active', -35.27851, 149.12691, 4, False, 'Canberra House, 40, Marcus Clarke Street, City, Canberra, District of Canberra Central, Australian Capital Territory, 2601, Australia'),
    ('LON03', 'EMEA', 'United Kingdom', 'One Canada Square', 'Active', 51.50495, -0.01951, 3, False, 'One Canada Square, 1, Canada Square, Canary Wharf, London Borough of Tower Hamlets, London, Greater London, England, E14 5AH, United Kingdom'),
    ('SYD03', 'APAC', 'Australia', 'Northpoint Tower', 'Active', -33.8688, 151.2093, 5, False, 'Northpoint Tower, 100 Miller St'),
    ('LAX02', 'AMER', 'USA', 'U.S. Bank Tower', 'Active', 34.05106, -118.25442, 6, False, 'U.S. Bank Tower, 633, Bunker Hill Steps, Bunker Hill, Downtown, Los Angeles, Los Angeles County, California, 90071, United States'),
    ('TOR02', 'AMER', 'Canada', 'TD Centre', 'Active', 43.69833, -79.43925, 4, False, 'TD Centre Parking Garage, Marlee Avenue, Little Jamaica, Eglinton—Lawrence, York, Toronto, Golden Horseshoe, Ontario, M6C 2E5, Canada'),
    ('BCN02', 'EMEA', 'Spain', 'Torre Mapfre', 'Active', 41.38778, 2.19735, 6, False, 'Torre Mapfre, Avinguda del Litoral, la Vila Olímpica del Poblenou, Sant Martí, Barcelona, Barcelonès, Barcelona, Catalonia, 08005, Spain'),
    ('BER02', 'EMEA', 'Germany', 'Potsdamer Platz', 'Active', 52.5098, 13.37559, 6, False, 'Potsdamer Platz, Tiergarten, Mitte, Berlin, 10785, Germany'),
    ('MEL03', 'APAC', 'Australia', '101 Collins St', 'Active', -37.8149, 144.97064, 2, False, '101 Collins Street, George Parade, East End Theatre District, Melbourne, City of Melbourne, Victoria, 3000, Australia'),
    ('CAL01', 'AMER', 'Canada', 'The Bow', 'Active', 51.0486, -114.06194, 4, False, 'The Bow, 500, 6 Avenue SE, Chinatown, Calgary, Alberta, T2G 1A6, Canada'),
    ('MEX02', 'AMER', 'Mexico', 'Torre Reforma', 'Active', 19.42482, -99.17444, 5, False, 'Torre Reforma, 483, Avenida Paseo de la Reforma, Cuauhtémoc, Mexico City, Cuauhtémoc, Mexico City, 06500, Mexico'),
    ('HOU02', 'AMER', 'USA', 'The Houstonian', 'Active', 29.76934, -95.46092, 4, False, 'The Houstonian, Stablewood Court, Stablewood, Houston, Harris County, Texas, 77024, United States'),
    ('ROM02', 'EMEA', 'Italy', 'EUR Business District', 'Active', 41.9028, 12.4964, 6, False, 'EUR Business District, Viale Europa 242'),
    ('SAO02', 'AMER', 'Brazil', 'Paulista Avenue', 'Active', 35.66811, 139.762, 3, False, 'Paulista Avenue, 1578'),
    ('AMS03', 'EMEA', 'Netherlands', 'WTC Amsterdam', 'Active', 52.33902, 4.87343, 3, False, 'Amsterdam Zuid, 10, Zuidplein, Zuidas, Zuid, Amsterdam, North Holland, Netherlands, 1077 XV, Netherlands'),
    ('VAN02', 'AMER', 'Canada', 'Bentall 5', 'Active', 49.28571, -123.11802, 2, False, 'Bentall 5, 550, Burrard Street, Downtown, Vancouver, Metro Vancouver Regional District, British Columbia, V6C 3A8, Canada'),
    ('MAD02', 'EMEA', 'Spain', 'Torre Espacio', 'Active', 40.4791, -3.68682, 5, False, 'Torre Emperador Castellana, 259 D, Paseo de la Castellana, La Paz, Fuencarral-El Pardo, Madrid, Community of Madrid, 28046, Spain'),
    ('BUE02', 'AMER', 'Argentina', 'Torre Alvear', 'Active', -34.59687, -58.99375, 4, False, 'La Torre, Alvear, La Fraternidad, Partido de General Rodríguez, Buenos Aires, B1748, Argentina'),
    ('LIM02', 'AMER', 'Peru', 'Torre Begonias', 'Active', -12.0921, -77.02396, 5, False, 'Torre Begonias, Calle Las Begonias, Centro financiero de San Isidro, San Isidro, Province of Lima, Lima Metropolitan Area, Lima, 15046, Peru'),
    ('WAW03', 'EMEA', 'Poland', 'Warsaw Trade Tower', 'Active', 52.2354, 20.98246, 4, False, 'Warsaw Trade Tower, 51, Chłodna, Mirów, Wola, Warsaw, Masovian Voivodeship, 00-867, Poland'),
    ('RIO02', 'AMER', 'Brazil', 'Edificio Argentina', 'Active', -22.94222, -43.18175, 6, False, 'Edifício Argentina, 228, Praia de Botafogo, Botafogo, Rio de Janeiro, Região Geográfica Imediata do Rio de Janeiro, Região Metropolitana do Rio de Janeiro, Região Geográfica Intermediária do Rio de Janeiro, Rio de Janeiro, Southeast Region, 22250-040, Brazil'),
    ('MIA02', 'AMER', 'USA', 'Southeast Financial Center', 'Active', 25.77222, -80.18769, 7, False, 'Southeast Financial Center, Southeast 3rd Street, Torch of Friendship, Miami, Miami-Dade County, Florida, 33131, United States'),
    ('PHI02', 'AMER', 'USA', 'Comcast Center', 'Active', 39.95472, -75.1685, 6, False, 'Comcast Center, 1701, John F. Kennedy Boulevard, Rittenhouse Square, Center City, Philadelphia, Philadelphia County, Pennsylvania, 19103, United States'),
    ('DUB02', 'EMEA', 'Ireland', 'The Capital Dock', 'Active', 53.3498, -6.2603, 4, False, "The Capital Dock, 79 Sir John Rogerson's Quay"),
    ('SIN03', 'APAC', 'Singapore', 'Marina Bay Sands', 'Active', 1.2837, 103.86072, 3, False, 'Marina Bay Sands, 10, Bayfront Avenue, Bayfront, Downtown Core, Central, Singapore, 018956, Singapore'),
    ('LJU01', 'EMEA', 'Slovenia', 'Crystal Palace', 'Active', 46.0569, 14.5058, 7, False, 'Crystal Palace, Trg republike 3'),
    ('SCL02', 'AMER', 'Chile', 'Costanera Center', 'Active', -33.41704, -70.60578, 5, False, 'Costanera Center, Nueva Tobalaba, Sanhattan, Providencia, Provincia de Santiago, Santiago Metropolitan Region, 7550099, Chile'),
    ('CPH02', 'EMEA', 'Denmark', 'Copenhagen Towers', 'Active', 55.62745, 12.57706, 7, False, 'Copenhagen Towers, Øresundsmotorvejen, Våren, Copenhagen, Copenhagen Municipality, Capital Region of Denmark, 1561, Denmark'),
    ('MON02', 'AMER', 'Canada', 'Tour CIBC', 'Active', 45.49859, -73.57108, 5, False, 'Tour CIBC, 1155, Boulevard René-Lévesque Ouest, Ville-Marie, Montreal, Urban agglomeration of Montreal, Montreal (administrative region), Quebec, H3B 4N4, Canada'),
    ('STO02', 'EMEA', 'Sweden', 'Stockholm Waterfront', 'Active', 59.32967, 18.05574, 4, False, 'Stockholm Waterfront, Klarabergsviadukten, Klara, Norrmalm, Norra innerstadens stadsdelsområde, Stockholm, Stockholm Municipality, Stockholm County, 111 64, Sweden'),
    ('AUS02', 'AMER', 'USA', 'Frost Bank Tower', 'Active', 30.26648, -97.74278, 5, False, 'Frost Bank Tower, 419, Congress Avenue, Downtown, Austin, Travis County, Texas, 78701, United States'),
    ('OSL02', 'EMEA', 'Norway', 'Barcode Project', 'Active', 59.9139, 10.7522, 3, False, 'Barcode Project, Dronning Eufemias gate'),
    ('CNS01', 'APAC', 'Australia', 'Cairns Corporate Tower', 'Active', -16.92532, 145.77791, 4, False, 'Cairns Corporate Tower, Lake Street, Cairns City, Cairns, Cairns Regional, Queensland, 4870, Australia'),
    ('BRU02', 'EMEA', 'Belgium', 'Finance Tower', 'Active', 50.85287, 4.36421, 4, False, 'Finance Tower, Boulevard du Jardin Botanique - Kruidtuinlaan, Brussels, Brussels-Capital, 1000, Belgium'),
    ('LIS02', 'EMEA', 'Portugal', 'Torre Vasco da Gama', 'Active', 38.77476, -9.09147, 5, False, 'Vasco da Gama Tower, Cais das Naus, Parque das Nações, Lisbon, 1990-173, Portugal'),
    ('CHC03', 'APAC', 'New Zealand', 'The Terrace', 'Active', -43.53259, 172.63448, 4, False, 'The Terrace, Central City, Linwood-Central-Heathcote Community, Christchurch, Christchurch City, Canterbury, New Zealand'),
    ('HEL02', 'EMEA', 'Finland', 'Sanoma House', 'Active', 60.1695, 24.9354, 7, False, 'Sanoma House, Töölönlahdenkatu 2'),
    ('PRG02', 'EMEA', 'Czech Republic', 'City Tower', 'Active', 50.05035, 14.43618, 4, False, 'City Tower, Hvězdova, Pankrác, Nusle, Prague, obvod Praha 4, Capital City of Prague, Prague, 140 63, Czechia'),
    ('BNE03', 'APAC', 'Australia', 'Waterfront Place', 'Active', -27.4703, 153.03052, 5, False, 'Waterfront Place, 1, Eagle Street, Golden Triangle, Brisbane City, Greater Brisbane, Queensland, 4000, Australia'),
    ('CBO02', 'AMER', 'Mexico', 'Plaza Pioneros', 'Active', 22.8905, -109.9167, 7, False, 'Plaza Pioneros, Blvd. Lázaro Cárdenas 3'),
    ('ARN02', 'EMEA', 'Sweden', 'Skrapan', 'Active', 59.31211, 18.07368, 4, False, 'Skrapan, Götgatan, Skanstull, Södermalm, Södermalms stadsdelsområde, Stockholm, Stockholm Municipality, Stockholm County, 118 30, Sweden'),
    ('EDI02', 'EMEA', 'United Kingdom', 'The Haymarket Edinburgh', 'Active', 55.94861, -3.21615, 6, False, 'The Haymarket, Haymarket, West End, City of Edinburgh, Scotland, EH3 8FP, United Kingdom'),
    ('MIL02', 'EMEA', 'Italy', 'Pirelli Tower', 'Active', 45.48478, 9.20118, 4, False, "Pirelli Tower, Piazza Duca d'Aosta, Centrale, Municipio 2, Milan, Lombardy, 20124, Italy"),
    ('PER03', 'APAC', 'Australia', 'QV.1 Building', 'Active', -31.9505, 115.8605, 7, False, 'QV.1 Building, 250 St Georges Terrace'),
    ('KAN02', 'AMER', 'USA', 'Town Pavilion', 'Active', 39.1006, -94.58261, 7, False, 'Town Pavilion, Main Street, Central Downtown, Central Business District KC, Downtown Kansas City, Kansas City, Jackson County, Missouri, 64105, United States'),
    ('SJC02', 'AMER', 'USA', 'Silicon Valley Center', 'Active', 37.3849, -121.92527, 7, False, 'Silicon Valley Center, North San Jose, San Jose, Santa Clara County, California, United States'),
    ('BUD02', 'EMEA', 'Hungary', 'Duna Tower', 'Active', 47.53514, 19.05927, 3, False, 'Duna Tower, 22, Népfürdő utca, Vizafogó lakótelep, Vizafogó, 13th district, Budapest, Central Hungary, 1376, Hungary'),
    ('BIR02', 'EMEA', 'United Kingdom', '103 Colmore Row', 'Active', 52.48086, -1.90146, 5, False, '103 Colmore Row, Digbeth, Park Central, Birmingham, West Midlands, England, United Kingdom'),
    ('POR02', 'AMER', 'USA', 'Wells Fargo Center', 'Active', 45.5142, -122.67892, 2, False, 'Wells Fargo Center, Downtown, Portland, Multnomah County, Oregon, United States'),
    ('BOI02', 'AMER', 'USA', 'Capitol Center', 'Active', 43.63304, -116.28553, 7, False, 'Capitol Care and Rehabilitation Center, 8211, West Ustick Road, West Bench Neighborhood Association, Boise, Ada County, Idaho, 83704, United States'),
    ('ATH02', 'EMEA', 'Greece', 'Athens Tower', 'Active', 37.98463, 23.76075, 4, False, 'Athens Tower 1, Σινώπης, Κουντουριώτικα, Ambelokipoi, 7th District of Athens, Athens, Municipality of Athens, Regional Unit of Central Athens, Attica, 115 27, Greece'),
    ('BKK02', 'APAC', 'Thailand', 'Baiyoke Tower II', 'Active', 13.75471, 100.54049, 7, False, 'Baiyoke Tower II, 222, Soi Ratchaprarop 5, Thanon Phaya Thai Subdistrict, Ratchathewi District, Bangkok, 10400, Thailand'),
    ('BCN03', 'EMEA', 'Spain', 'Torre Glòries', 'Active', 41.40352, 2.18952, 5, False, 'Glòries Tower, Carrer de Badajoz, el Parc i la Llacuna del Poblenou, Sant Martí, Barcelona, Barcelonès, Barcelona, Catalonia, 08018, Spain'),
    ('MUN02', 'EMEA', 'Germany', 'Highlight Towers', 'Active', 48.1766, 11.59273, 7, False, 'Highlight Towers, 6, Mies-van-der-Rohe-Straße, Parkstadt Schwabing, Alte Heide - Hirschau, Schwabing-Freimann, Munich, Bavaria, 80807, Germany'),
    ('GLA02', 'EMEA', 'United Kingdom', 'The Hub', 'Active', 55.85615, -4.29572, 3, False, 'The Hub, Pacific Quay, Stobcross, Cessnock, Glasgow, Glasgow City, Scotland, G51 1EA, United Kingdom'),
    ('VIE02', 'EMEA', 'Austria', 'DC Tower', 'Active', 48.23142, 16.41474, 5, False, 'DC Tower 3, 3, Donau-City-Straße, Donau City, KG Kaisermühlen, Donaustadt, Vienna, 1220, Austria'),
    ('DUS02', 'EMEA', 'Germany', 'Stadttor', 'Active', 51.21559, 6.76086, 4, False, 'Stadttor, Unterbilk, Stadtbezirk 3, Dusseldorf, North Rhine-Westphalia, 40221, Germany'),
    ('LPL02', 'EMEA', 'United Kingdom', 'West Tower', 'Active', 53.41003, -2.99658, 6, False, 'West Tower, Brook Street, Pride Quarter, Vauxhall, Liverpool, Liverpool City Region, England, L3 9BP, United Kingdom'),
]

# (name, region, country, facility, status, lat, long, switch_platform, address)
_DATA_CENTER_ROWS = [
    ('ASH01', 'AMER', 'USA', 'Equinix DC1', 'Active', 39.04372, -77.48749, 'Arista EOS', '21715 Filigree Court, Ashburn, Virginia, 20147, United States'),
    ('FRA01', 'EMEA', 'Germany', 'Equinix FR5', 'Active', 50.13862, 8.73907, 'Cisco NX-OS', 'Kruppstraße 121-127, 60388 Frankfurt am Main, Germany'),
    ('TYO01', 'APAC', 'Japan', 'Equinix TY2', 'Active', 35.61978, 139.7454, 'Arista EOS', '3-8-1 Higashi-Shinagawa, Shinagawa-ku, Tokyo, 140-0002, Japan'),
]

BRANCHES = [
    Site(
        site_name=r[0],
        region_name=r[1],
        country_name=r[2],
        site_facility=r[3],
        site_status=r[4],
        site_latitude=r[5],
        site_longitude=r[6],
        site_access_switch_count=r[7],
        has_experimental_sdwan_deployment=r[8],
        site_address=r[9],
    )
    for r in _BRANCH_ROWS
]

DATA_CENTERS = [
    Site(
        site_name=r[0],
        region_name=r[1],
        country_name=r[2],
        site_facility=r[3],
        site_status=r[4],
        site_latitude=r[5],
        site_longitude=r[6],
        switch_platform=r[7],
        site_address=r[8],
    )
    for r in _DATA_CENTER_ROWS
]


class DesignFailed(Exception):
    """Raised when one of the underlying design jobs fails."""


class SeedDemoData(Job):
    """Run the NDG Design Builder jobs in dependency order to build a demo dataset."""

    branch_count = IntegerVar(
        default=2,
        min_value=0,
        max_value=215,
        label="Branch count",
        description=(
            "How many branch sites to build (NDG ships 215 definitions). "
            "demo.nautobot.com sizes are 2 (small) and 80 (large). Each branch is a "
            "separate design run plus one run per access switch, so large values take hours."
        ),
    )
    include_backbone = BooleanVar(default=True, label="Include backbone")
    include_cloud = BooleanVar(default=True, label="Include cloud")
    include_datacenters = BooleanVar(default=True, label="Include data centers")
    include_networktocode = BooleanVar(
        default=False,
        label="Include NetworkToCode sites",
        description=(
            "The NTC stages additionally require nautobot-golden-config and "
            "nautobot-dns-models. Stage 2 deletes and recreates 'Network to Code' "
            "tenant IP addresses, because those assignments are not idempotent."
        ),
    )
    sync_git_repositories = BooleanVar(
        default=False,
        label="Sync Git repositories afterwards",
        description="Re-sync every configured Git repository once seeding completes.",
    )
    fail_fast = BooleanVar(
        default=True,
        label="Stop on first failure",
        description=(
            "Abort as soon as a design fails. Turn this off to keep going and collect "
            "every failure, though later stages will likely fail too since they depend "
            "on earlier ones."
        ),
    )

    class Meta:
        """Metadata for SeedDemoData."""

        name = "Seed Demo Data (NDG designs)"
        description = "Build the demo.nautobot.com dataset by running the NDG Design Builder jobs in order."
        has_sensitive_variables = False
        # Seeding a large dataset runs well past the default soft time limit.
        soft_time_limit = 43200
        time_limit = 43500

    def __init__(self, *args, **kwargs):
        """Track failures and the discovered NDG module prefix across the run."""
        super().__init__(*args, **kwargs)
        self.failures = []
        self._fail_fast = True
        self._prefix = ""

    # ------------------------------------------------------------------
    # Locating NDG
    # ------------------------------------------------------------------

    def _resolve_module_prefix(self):
        """Find the prefix NDG's design modules are registered under.

        Jobs loaded from JOBS_ROOT register as `jobs.designs.branch`, while jobs synced
        from a Git repository are namespaced under the repository slug, e.g.
        `nautobot-data-generation.jobs.designs.branch`. Look up a design that must exist
        and derive the prefix from whatever it is actually called.
        """
        submodule, class_name = NDG_SENTINEL
        match = (
            JobModel.objects.filter(module_name__endswith=submodule, job_class_name=class_name)
            .order_by("module_name")
            .first()
        )
        if match is None:
            raise DesignFailed(
                f"Could not find the NDG design job {class_name} (expected a module ending in "
                f"'{submodule}'). Confirm the nautobot-data-generation repository is synced as a "
                "Git repository with 'Provides: jobs', that its sync succeeded, and that "
                "nautobot-design-builder is installed and enabled."
            )

        self._prefix = match.module_name[: -len(submodule)]
        if self._prefix:
            self.logger.info("Found NDG design jobs under module prefix `%s`", self._prefix)
        else:
            self.logger.info("Found NDG design jobs at their bare module paths")

    # ------------------------------------------------------------------
    # Design execution
    # ------------------------------------------------------------------

    def _enable_ndg_jobs(self):
        """Enable the NDG design jobs, which sync from Git in a disabled state."""
        modules = [f"{self._prefix}{submodule}" for submodule in NDG_SUBMODULES]
        enabled = JobModel.objects.filter(module_name__in=modules, enabled=False).update(enabled=True)
        if enabled:
            self.logger.info("Enabled %s NDG design job(s)", enabled)

    def _run_design(self, submodule, job_class_name, **job_kwargs):
        """Run one design job synchronously and raise if it fails."""
        module = f"{self._prefix}{submodule}" if submodule.startswith("jobs.") else submodule
        try:
            job_model = JobModel.objects.get(module_name=module, job_class_name=job_class_name)
        except JobModel.DoesNotExist:
            message = (
                f"Design job {module}.{job_class_name} is not registered. Confirm the "
                "nautobot-data-generation repository is synced as a Git repository "
                "providing 'jobs', and that nautobot-design-builder is installed."
            )
            return self._record_failure(f"{module}.{job_class_name}", message)

        label = job_model.name or f"{module}.{job_class_name}"
        self.logger.info("Running design **%s**", label)

        job_result = JobResult.enqueue_job(
            job_model,
            self.user,
            synchronous=True,
            job_kwargs={"dryrun": False, **job_kwargs},
        )
        job_result.refresh_from_db()

        if job_result.status != JobResultStatusChoices.STATUS_SUCCESS:
            return self._record_failure(label, f"status {job_result.status}", job_result)

        self.logger.info("Completed design **%s**", label)
        return job_result

    def _record_failure(self, label, message, job_result=None):
        """Log a design failure, and abort the run when fail_fast is set."""
        detail = f"Design **{label}** failed: {message}"
        self.logger.error(detail)
        if job_result is not None and job_result.traceback:
            self.logger.error("Traceback for %s:\n%s", label, job_result.traceback)
        self.failures.append(detail)
        if self._fail_fast:
            raise DesignFailed(detail)
        return job_result

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(  # pylint: disable=arguments-differ,too-many-arguments,too-many-branches
        self,
        branch_count,
        include_backbone,
        include_cloud,
        include_datacenters,
        include_networktocode,
        sync_git_repositories,
        fail_fast,
    ):
        """Execute each requested stage in dependency order."""
        self._fail_fast = fail_fast
        self.failures = []

        self._resolve_module_prefix()
        self._enable_ndg_jobs()
        branches, data_centers = self._load_site_definitions()

        self._seed_branches(branches, branch_count)

        if include_backbone:
            # Backbone must be cabled after it is created.
            self._run_design("jobs.designs.backbone", "CreateBackbone")
            self._run_design("jobs.designs.backbone", "CableBackbone")

        if include_cloud:
            self._run_design("jobs.designs.cloud", "CreateCloud")

        if include_datacenters:
            self._seed_datacenters(data_centers)

        if include_networktocode:
            self._seed_networktocode()

        if sync_git_repositories:
            self._sync_repositories()

        if self.failures:
            # Every failure is already in the job log; keep the summary short.
            shown = self.failures[:5]
            summary = "; ".join(shown)
            if len(self.failures) > len(shown):
                summary += f"; ... and {len(self.failures) - len(shown)} more (see the job log)"
            raise DesignFailed(f"{len(self.failures)} design(s) failed: {summary}")

        return "Demo data seeding complete."

    # ------------------------------------------------------------------
    # Stages
    # ------------------------------------------------------------------

    def _seed_branches(self, branches, branch_count):
        """Build the branch base plus `branch_count` branch sites and their switches."""
        # Stage 1 builds the shared base objects every branch design references.
        self._run_design("jobs.designs.branch", "CreateBase")

        remaining = branch_count
        for site in branches:
            if remaining <= 0:
                break
            # Skip sites already present so the job is re-runnable.
            if Location.objects.filter(name=site.site_name).exists():
                self.logger.info("Skipping existing branch %s", site.site_name)
                continue

            self._run_design(
                "jobs.designs.branch",
                "CreateBranch",
                site_name=site.site_name,
                region_name=site.region_name,
                country_name=site.country_name,
                site_facility=site.site_facility,
                status=site.site_status,
                site_latitude=str(round(site.site_latitude, 5)),
                site_longitude=str(round(site.site_longitude, 5)),
                physical_address=site.site_address,
                has_experimental_sdwan_deployment=str(site.has_experimental_sdwan_deployment),
            )

            # Count the attempt either way, so a failing design cannot turn a request
            # for 2 branches into a walk through all 215 definitions.
            remaining -= 1

            location = Location.objects.filter(name=site.site_name).first()
            if location is None:
                # CreateBranch failed but fail_fast is off; skip its access switches.
                self.logger.warning("Branch %s was not created; skipping its access switches", site.site_name)
                continue

            for _ in range(site.site_access_switch_count):
                self._run_design(
                    "jobs.designs.branch",
                    "CreateAccessSwitch",
                    location=str(location.pk),
                    status=site.site_status,
                )

    def _seed_datacenters(self, data_centers):
        """Build the data-center base and each DC's switches, load balancers and VMs."""
        self._run_design("jobs.designs.datacenter", "CreateDataCenterBase")

        for site in data_centers:
            self._run_design(
                "jobs.designs.datacenter",
                "CreateDataCenter",
                site_name=site.site_name,
                region_name=site.region_name,
                country_name=site.country_name,
                site_facility=site.site_facility,
                status=site.site_status,
                site_latitude=str(round(site.site_latitude, 5)),
                site_longitude=str(round(site.site_longitude, 5)),
                physical_address=site.site_address,
            )

            location = Location.objects.filter(name=site.site_name).first()
            if location is None:
                self.logger.warning("Data center %s was not created; skipping its stages", site.site_name)
                continue

            self._run_design(
                "jobs.designs.datacenter",
                "CreateDataCenterSwitch",
                location=str(location.pk),
                switch_platform=site.switch_platform,
            )
            self._run_design("jobs.designs.datacenter", "CreateDataCenterLoadBalancer", location=str(location.pk))
            self._run_design("jobs.designs.datacenter", "CreateDataCenterVM", location=str(location.pk))

    def _seed_networktocode(self):
        """Run the NetworkToCode stages, which depend on golden-config and dns-models."""
        from nautobot.ipam.models import IPAddress  # noqa: PLC0415

        self._run_design("jobs.designs.networktocode", "CreateNetworkToCode")

        # Stage 2's IP assignments are not idempotent, so clear them first.
        # This mirrors render_networktocode_designs and only touches the NTC tenant.
        deleted, _ = IPAddress.objects.filter(tenant__name="Network to Code").delete()
        if deleted:
            self.logger.info("Removed %s existing 'Network to Code' IP address record(s) before re-seeding", deleted)

        for site in NTC_SITES:
            self._run_design("jobs.designs.networktocode", "CreateNetworkToCodeSite", **site)

        self._run_design("jobs.designs.networktocode", "CreateNetworkToCodeGoldenConfig")
        self._run_design("jobs.designs.networktocode", "CreateNetworkToCodeDnsModels")
        self._run_design("jobs.designs.vpn", "CreateVPN")

    def _sync_repositories(self):
        """Sync every Git repository, as the NDG command does after seeding."""
        for repo in GitRepository.objects.all():
            self.logger.info("Syncing Git repository %s", repo.name)
            self._run_design("nautobot.core.jobs", "GitRepositorySync", repository=str(repo.pk))

    # ------------------------------------------------------------------
    # Site data
    # ------------------------------------------------------------------

    def _load_site_definitions(self):
        """Return the NDG branch and data-center site definitions.

        Upstream these live in the `nautobot_gizmo_designs` package, which sits outside
        the NDG repo's `jobs/` directory and so is not importable when only `jobs/` is
        synced as a Git job source. Prefer the upstream copy when it is available so the
        data stays current, and fall back to the tables defined in this file.
        """
        try:
            from nautobot_gizmo_designs.management.commands import (  # noqa: PLC0415
                BRANCHES as upstream_branches,
                DATA_CENTERS as upstream_data_centers,
            )
        except ImportError:
            self.logger.info(
                "`nautobot_gizmo_designs` is not importable; using the %s branch and %s data center "
                "definitions bundled in this file.",
                len(BRANCHES),
                len(DATA_CENTERS),
            )
            return BRANCHES, DATA_CENTERS

        self.logger.info("Using site definitions from the installed nautobot_gizmo_designs package")
        return upstream_branches, upstream_data_centers


register_jobs(SeedDemoData)

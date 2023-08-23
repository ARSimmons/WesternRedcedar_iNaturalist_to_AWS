import requests
import psycopg2
from config import config


class Observations:
    def __init__(self):
        per_page = 20
        observations = []

        url = (
            "https://api.inaturalist.org/v1/observations?project_id=western-redcedar-dieback-map&page="
            + str(1)
            + "&per_page="
            + str(per_page)
            + "&order=desc&order_by=created_at"
        )

        total_results = requests.get(url).json()["total_results"]
        total_pages_to_fetch = (total_results + per_page - 1) // per_page

        print("Fetching Observations")
        print("Results: ", total_results)
        print("Pages to Fetch: ", total_pages_to_fetch)

        for page in range(1, total_pages_to_fetch+1):
            print("Page ", page)
            url = (
                "https://api.inaturalist.org/v1/observations?project_id=western-redcedar-dieback-map&page="
                + str(page)
                + "&per_page="
                + str(per_page)
                + "&order=desc&order_by=created_at"
            )
            json_data = requests.get(url).json()
            page_observations = json_data["results"]
            observations.extend(page_observations)

        self.count = json_data["total_results"]
        self.rawObs = observations
        # print(observations[0])

    def parse(self):
        print("Parsing observations")

        self.simpleObs = []

        for ob in self.rawObs:
            sob = {}

            fields = [
                "id",
                "observed_on_string",
                "observed_on",
                "time_observed_at",
                "observed_time_zone",
                "created_at",
                "updated_at",
                "quality_grade",
                "license_code",
                "uri",
                "user/id",
                "user/login",
                "photos/0/url",
                "description",
                "num_identification_agreements",
                "num_identification_disagreements",
                "captive",
                "oauth_application_id",
                "place_guess",
                "geojson/coordinates/1",
                "geojson/coordinates/0",
                "positional_accuracy",
                "geoprivacy",
                "public_positional_accuracy",
                "taxon_geoprivacy",
                #"coordinates_obscured",
                #"positioning_method",
                #"positioning_device",
            ]

            for i in range(15):
                fields.append(f"ofvs/{i}/name")
                fields.append(f"ofvs/{i}/value")

            for field_path in fields:
                self.copyField(sob, ob, field_path)

            self.organize_ofvs(sob)

            try:
                if "photos" in sob and sob["photos"]:
                    sob["photos"][0]["url"] = sob["photos"][0]["url"].replace(
                        "square", "original"
                    )
            except Exception:
                pass

            #print(sob)

            self.simpleObs.append(sob)

    def copyField(self, sob, ob, field_path):
        strings_path = field_path.split("/")
        path = []

        for part in strings_path:
            try:
                path.append(int(part))
            except Exception:
                path.append(part)

        new_sob = sob
        for part in path[:-1]:
            if part not in new_sob.keys():
                new_sob[part] = {}
            new_sob = new_sob[part]
        new_sob[path[-1]] = None

        try:
            new_ob = ob
            new_sob = sob
            for part in path[:-1]:
                new_ob = new_ob[part]
                new_sob = new_sob[part]
            new_sob[path[-1]] = new_ob[path[-1]]

        except Exception as e:
            # print("Error",e)
            pass

    def organize_ofvs(self, ob):
        new_ofvs = {}

        for i in range(len(ob["ofvs"])):
            name = ob["ofvs"][i]["name"]
            value = ob["ofvs"][i]["value"]
            new_ofvs[name] = value

        ob["ofvs"] = new_ofvs

    def flattenObservation(self,obs):
        nexts = [("",obs)]
        flat_titles = {}
        while True:
            if len(nexts):
                current_name, current_object = nexts.pop() 
                try:
                    for key in current_object.keys():
                        if key == "ofvs":
                            continue
                        new_name = ""
                        if len(current_name) > 0:
                            new_name = current_name + "_"
                        new_name += str(key)
                        nexts.append((new_name,current_object[key]))
                except Exception as e:
                    flat_titles[current_name] = current_object
            else:
                break
        flat_titles["ofvs"] = None#str(obs["ofvs"])
        return flat_titles


def connect(obs):
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print("connecting to the postgreSQL db ===")
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print("postgreSQL db version ===")
        cur.execute("SELECT version()")
        

        for ob in obs.simpleObs:

            varlist = []
            titles_list = []
            
            observation_flat = obs.flattenObservation(ob)
            for key, value in observation_flat.items():
                titles_list.append(key)
                varlist.append(value)
            

            variables = ', '.join(["%s" for var in varlist[:-1]])
            titles = ', '.join([str(var) for var in titles_list[:-1]])

            pairs_update = ', '.join([str(var) + "=%s" for var in titles_list[:-1]])

            query_string = """UPDATE inaturalist SET %s WHERE id=%s""" % (pairs_update,observation_flat['id'])
            
            cur.execute(query_string,varlist[:-1])
            
            query_string = """INSERT INTO inaturalist (%s) VALUES (%s) ON CONFLICT (id) DO NOTHING""" % (titles,variables)
     
            cur.execute(query_string,varlist[:-1])

            query_string = """UPDATE inaturalist SET geom = ST_SetSRID(ST_MakePoint(geojson_coordinates_0, geojson_coordinates_1), 4326);"""
            cur.execute(query_string)

            conn.commit()


        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("error ===", error)
    finally:
        if conn is not None:
            conn.close()
            print("db connection closed ===")


if __name__ == "__main__":
    obs = Observations()
    obs.parse()
    connect(obs)

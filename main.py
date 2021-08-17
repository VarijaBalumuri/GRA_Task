import requests
import json


class Analyzedata(object):

    def extract_data_u1(self, ds, actual_data):
        ds["ds_short_name"] = actual_data.get("ds_short_name")
        ds["dataset_name"] = actual_data.get("dataset_name")
        ds["Authors"] = actual_data.get("Authors")
        ds["start_date"] = actual_data.get("start_date")
        ds["stop_date"] = actual_data.get("stop_date")
        ds["format"] = actual_data.get("format")
        if actual_data.get("platform") and len(actual_data.get("platform")) > 0:
            ds["platform"] = actual_data.get("platform")[0]
        if actual_data.get("coll_name") and len(actual_data.get("coll_name")) > 0:
            ds["coll_name"] = actual_data.get("coll_name")[0]
        if actual_data.get("project_home_page") and len(actual_data.get("project_home_page")) > 0:
            ds["project_home_page"] = actual_data["project_home_page"]["urls"][0]["url"]
        return ds

    def populate_data_u1(self, data_list):
        final_list = []
        for each_data in data_list:
            ds = dict(
                ds_short_name="",
                platform="",
                Authors="",
                start_date="",
                stop_date="",
                format="",
                coll_name="",
                proj_name="",
                dataset_name="",
                project_home_page="",
            )
            fitem = self.extract_data_u1(ds, each_data["_source"])
            final_list.append(fitem)
        return final_list

    def get_response(self, url):
        with requests.Session() as session:
            response = session.get(url)

        # response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print("Unable to get the data")

    def get_total_dataset_count(self,datasetcountqueryurl):
        data = self.get_response(datasetcountqueryurl)
        if data.get("aggregations") and len(data.get("aggregations")) > 0:
            datasetcount = data["aggregations"]["COUNT(*)"]["value"]
        datasetvariableurl = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql=SELECT * from ghrc limit {}".format(datasetcount)
        ghrc_inv_query = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql=SELECT * from ghrc_inv where ds_short_name='{}'"
        self.get_data(datasetvariableurl, ghrc_inv_query)

    def get_data(self, url, template):
        import time
        start_time = time.time()
        url = url
        template = template

        first_url_dataset_list = self.get_data_u1(url)
        all_dataset_len = 0
        for times, each_first_url_data in enumerate(first_url_dataset_list):
            print("START {}\n".format(times))
            second_data_url = template.format(each_first_url_data["ds_short_name"])
            second_data_list= self.get_data_u2(second_data_url)
            each_first_url_data["inventory_data"] = second_data_list
            all_dataset_len += len(second_data_list)
            print("\t \t File Count for each dataset : {}".format(len(second_data_list)))

        print("----- Total dataset File Count : {} -----".format(all_dataset_len))
        print("Total time the process took : {}".format(time.time() - start_time))
        self.write_data(first_url_dataset_list, "final.json")

    def get_data_u1(self, url):
        data = self.get_response(url)
        df = self.populate_data_u1(data["hits"]["hits"])
        return df

    def get_data_u2(self, url):
        print("Getting value for URL: {}".format(url))
        data = self.get_response(url)
        df = []
        if data and data.get("hits") and len(data["hits"]["hits"]) > 0:
            df = self.populate_data_u2(data)
        else:
            print("\t no hits")
        return df

    def get_download_links(self, url,datasetname):
        print(url.format(datasetname))
        data = self.get_response(url.format(datasetname))
        df = []
        if data and data.get("hits") and len(data["hits"]["hits"]) > 0:
            df = self.populate_download_links(data)
            print("---download Links----{}".format(df))
        else:
            print("\t no hits")
        return df

    def populate_download_links(self,data):
        return_data = []
        if data and data.get("hits") and len(data["hits"]["hits"]) > 0:

            actual_data = data["hits"]["hits"]
            for d in actual_data:
                ds = dict(
                    path="",
                )
                source_data = d["_source"]
                ds["path"] = source_data["path"]
                return_data.append(ds)
            return return_data

    def populate_data_u2(self, data):
        return_data = []
        if data and data.get("hits") and len(data["hits"]["hits"]) > 0:

            actual_data = data["hits"]["hits"]
            for d in actual_data:
                ds = dict(
                    format="",
                    granule_name="",
                    data_access="",
                    checksum="",
                    file_size=""
                )
                source_data = d["_source"]
                ds["granule_name"] = source_data["granule_name"]
                ds["data_access"] = source_data["data_access"]
                ds["checksum"] = source_data["checksum"]
                ds["file_size"] = source_data["file_size"]
                return_data.append(ds)
        return return_data

    def write_data(self, df, filename):
        with open(filename, 'w') as fp:
            json.dump(df, fp)

    def get_custom_data_query(self, template, key, value):
        data = self.get_response(template.format(key, value))
        print("---Format the data sets on given user input ---{}".format(data))
        return data

    def get_custom_daterange_query(self, template):
        data = self.get_response(template)
        print("---Date Range DataSets ---{}".format(data))
        return data

totaldatasetcountqueryurl = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql=SELECT count(*) from ghrc"
datarangedatasetquerytemplate = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql=SELECT * from ghrc_inv where start_date>='2004-08-06 00:00:00' and start_date<='2007-01-01 00:00:00'"
formatquerytemplate = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql=SELECT * from ghrc_inv where {}='{}'"
downloadlinksquery = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql=SELECT * from ghrc_inv where ds_short_name='{}'"

object = Analyzedata()

object.get_total_dataset_count(totaldatasetcountqueryurl)
object.get_custom_data_query(formatquerytemplate, 'format', "ASCII")
# we can pass the key value pairs here as well for given user input by passing key value
object.get_custom_daterange_query(datarangedatasetquerytemplate)
object.get_download_links(downloadlinksquery,'c4gandros')

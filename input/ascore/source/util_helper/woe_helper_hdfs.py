import pickle

# import matplotlib.pyplot as plt
import pyspark.sql.functions as F


class WOEtransformer():
    def __init__(self):
        pass

    def parse_string(self, string):
        res = string[8:-2]
        res = res.split(' IN (')
        if len(res) <= 1:
            return res[0]
        else:
            output = res[0]
            for istr in res[1:]:
                isubstr = istr.split(')', 1)
                if '{' in isubstr[0] and '}' in isubstr[0]:
                    output += ' IN ('
                    output += ', '.join([f'''"{x}}}"''' for x in isubstr[0].split('}, ')])
                    output = output[:-2] + output[-1]
                    output += ')' + isubstr[1]
                else:
                    output += ' IN ('
                    output += ', '.join([f'''"{x}"''' for x in isubstr[0].split(', ')])
                    output += ')' + isubstr[1]
            return output

    def load(self, filename, spark):
        binary_data = spark.sparkContext.binaryFiles(filename).collect()[0][1]
        foo = pickle.loads(binary_data)

        # with open(filename, 'rb') as f:
        #    foo = pickle.load(f)

        transformer_bin = foo[1]
        transformer_woe = foo[2]
        transformer_bin_dict, transformer_woe_dict = {}, {}

        for istr in transformer_bin:
            feature_name = istr.split('END AS `')[1].split('`')[0]
            transformer_bin_dict[feature_name] = self.parse_string(istr)

        for istr in transformer_woe:
            feature_name = istr.split('END AS `')[1].split('`')[0]
            transformer_woe_dict[feature_name] = self.parse_string(istr)

        self.mapping_transformation = foo[0]
        self.transformer_bin_dict = transformer_bin_dict
        self.transformer_woe_dict = transformer_woe_dict

    def transform(self, sdf, labelCol, excludeCols):
        spark_transformer_bins = [F.expr(self.transformer_bin_dict[icol]) for icol in sdf.columns if
                                  icol not in [labelCol] + excludeCols and icol in self.transformer_bin_dict]
        spark_transformer_woe = [F.expr(self.transformer_woe_dict[icol]) for icol in sdf.columns if
                                 icol not in [labelCol] + excludeCols and icol in self.transformer_woe_dict]
        return sdf.select(labelCol, *excludeCols, *spark_transformer_bins).select(labelCol, *excludeCols,
                                                                                  *spark_transformer_woe)

    # def plot(self, feature):
    #     woe_summary = self.mapping_transformation
    #     plot_woe(woe_summary, feature, metric="WOE")

    def show(self, feature):
        woe_summary = self.mapping_transformation
        return show(woe_summary, feature)


# def plot_woe(woe_summary, feature, metric="WOE"):
#     df = woe_summary[woe_summary["Feature"] == feature].copy()
#     # Truncate Column Value
#     df["Value"] = df["Value"].astype("string").apply(lambda x: x if len(x) <= 15 else x[:15] + '...')
#
#     df_missing = df[df["Value"] == "Missing"]
#     df_nonmissing = df[df["Value"] != "Missing"]
#     iv = round(df["iv"].iloc[0], 4)
#
#     # create figure and axis objects with subplots()
#     fig, ax = plt.subplots()
#
#     plt.xticks(rotation=45)
#
#     # make a plot
#     ax.plot(df_missing["Value"].astype("string"), df_missing[metric], color="red", marker="o")
#     ax.plot(df_nonmissing["Value"].astype("string"), df_nonmissing[metric], color="red", marker="o")
#     # annotate with value
#     for i, j in zip(df["Value"].astype("string"), df[metric]):
#         ax.annotate(str(round(j, 4)), xy=(i, j))
#     # set x-axis label
#     ax.set_xlabel(f"{feature}, IV = {iv}", fontsize=14)
#     # set y-axis label
#     ax.set_ylabel(metric, color="red", fontsize=14)
#
#     ax2 = ax.twinx()
#     # make a plot with different y-axis using second axis object
#     ax2.bar(df["Value"].astype("string"), df["#Obs"], edgecolor="blue", fill=False)
#     ax2.set_ylabel("#Obs", color="blue", fontsize=14)
#
#     plt.plot()


def show(woe_summary, feature):
    return woe_summary[woe_summary['Feature'] == feature]

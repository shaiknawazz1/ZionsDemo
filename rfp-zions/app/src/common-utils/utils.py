

def spark_to_csv(df, file_path):
    """ Converts spark dataframe to CSV file """
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=df.columns)
        writer.writerow(dict(zip(fieldnames, fieldnames)))
        for row in df.toLocalIterator():
            writer.writerow(row.asDict())
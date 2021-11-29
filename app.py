import os
from util import get_spark_session
from read import from_files
from process import transform
from write import to_file

def main():
    # env = os.environ.get('ENVIRON')
    env = 'DEV'
    # src_dir = os.environ.get('SRC_DIR')
    src_dir = '/home/bartonkyler048_gmail_com/itv_ghactivity/data/itv-github/landing/ghactivity'
    # file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    file_pattern = '2021-01-13-*'
    # src_file_format = os.environ.get('SRC_FILE_FORMAT')
    src_file_format = 'json'
    # tgt_dir = os.environ.get('TGT_DIR')
    tgt_dir = '/home/bartonkyler048_gmail_com/itv_ghactivity/data/itv-github/raw/ghactivity'
    # tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    tgt_file_format = 'parquet'
    spark = get_spark_session(env, 'GitHub Activity - Reading Data')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed = transform(df)
    # df_transformed.printSchema()
    # df_transformed.select('year', 'month', 'day').show(truncate=False)
    to_file(df_transformed, tgt_dir, tgt_file_format)


if __name__ == '__main__':
    main()


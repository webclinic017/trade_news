# Trade news project summary

Trade news is a diploma project. A short presentation is located in `presentation` folder.
The project is about stock price prediction. We propose news features which represent news event types and are based on fine-tuned BERT classifier.

These notebooks allows you to:
- Collect news and prices with Thomson Reuters API
- Use our manual labelling to fine tune BERT 
- Generate features, build models
- Assess the effect of including proposed news features to each model

As a result of our research, proposed news features improve model evaluation metrics and profitability.
For example, profitability:
![Mean profit](https://github.com/dany-kuznetsov/trade_news/blob/master/presentation/models_ptofit.jpg?raw=true)

# Data 
This directory is not complete. The data folder can be downloaded with this link:
https://drive.google.com/drive/folders/1wWjm-IjKMdXdGrCFPnjAbSJ8nZLbtQK9?usp=sharing

# Notebooks and scripts description
The whole process can be described by the scheme:

![Notebooks Scheme](https://github.com/dany-kuznetsov/trade_news/blob/master/presentation/The%20scheme%20of%20notebooks%20from%20data%20collection%20to%20modelling.jpg?raw=true)

## Script - `trade_news_predict_prices.py`
The script contains a set of functions for
- data collecting
- data processing
- working with file system
- feature generation

## Notebook - `news_collector`
You can get news of your companies by changing `rics_to_loop_df` param of `get_headlines_and_full_text_news_save` function.
Also you can get recent news up to selected date in `Lines of code to update all_headlines_df with recent news` section.

## Notebook - `stock_prices_collector`
Similarly your list of companies can be used to collect prices.

## Notebook - `key_words_matching`
We prepared set of rules for each news event type - `data/key_words.xlsx`.
We used these regexp rules to generate excel files with basic key words labelling.

These output excel files with most representative news texts for each event type were manually labelled. 
Additionally, there are some graph analysis and cluster analysis of event types intersections based on basic key words classification.

As a result, `data/for_labelling/` folder contains files with key words labelling and `data/labeled_news_iter1/` folder contains files with manual labelling.

## Notebook - `create_merged_file_with_labels`
This notebook mergea several files of manual labelling in `data/merged_news_and_prices/` folder.
The output is a file `data/lower_labelled_texts_list_types.csv`. It used for fine tuning BERT classifier.

## Notebook - `bert_labelling`
Fine tuning pre-trained BERT model with manually labelled texts.
Then use BERT to label other unlabelled texts.

## Notebook - `merge_news_prices_to_daily_df`
Merge news featrues for each day with prices (open, close, min, max price of day)

## Notebook - `model_pipeline`
The data on that step contains news and prices features for each day. It's a time to build models.
- Feature generation
- Data transformation for classic ML models
- Data transformation for deep learning models
- Build regression, boosting and LSTM models
- Compare models with only prices or with news


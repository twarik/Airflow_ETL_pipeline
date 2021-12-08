#Import libraries.
import pandas as pd
import geopandas as gpd
import json, urllib
import base64

import matplotlib.pyplot as plt
from bokeh.plotting import figure
from bokeh.models import GeoJSONDataSource
from bokeh.palettes import Spectral6
from bokeh.transform import factor_cmap


from sqlalchemy import create_engine

import streamlit as st
st.set_option('deprecation.showPyplotGlobalUse', False)

engine = create_engine('postgres+psycopg2://postgres:pass1234@localhost:5432/hence_db')


@st.cache(allow_output_mutation=True, ttl=60*60)# Refresh and Clear Cache hourly
def load_data():
	'''
	Function to query the DHS Program API for surveys data.
	'''
	surveys_url = r'https://api.dhsprogram.com/rest/dhs/surveys'
	#Obtain and Parse the list into a Python Object.
	req = urllib.request.urlopen(surveys_url)
	resp = json.loads(req.read())
	surveys_data = resp['Data']

	data = pd.DataFrame.from_dict(surveys_data)
	data.SurveyYear = pd.to_numeric(data.SurveyYear)# change the column type to numeric
	return data

def download_link(object_to_download, download_filename, download_link_text):
    """
    Function to generate a link to download the surveys as csv.
    """
    if isinstance(object_to_download,pd.DataFrame):
        object_to_download = object_to_download.to_csv(index=False)

    # some strings <-> bytes conversions necessary here
    b64 = base64.b64encode(object_to_download.encode()).decode()

    return f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'

def map():
	sql= "select * from ug_regions"
	df=gpd.GeoDataFrame.from_postgis(sql,con=engine,geom_col='geometry' )

	sql2= "select * from ug_clusters"
	df2=gpd.GeoDataFrame.from_postgis(sql2,con=engine)
	# Remove outliers
	df2 = df2[df2['geom'].x > 25]
	# 3 • Clusters in each region of Uganda?
	p = figure(
 		title='Map of Uganda showing regions of the Country and cluster locations',
		x_axis_label='Longitude',
		y_axis_label='Latitude')
	p.circle(df2['geom'].x, df2['geom'].y)

	geojson = df.to_json()
	geo_source = GeoJSONDataSource(geojson=geojson)
	p.patches(xs='xs', ys='ys', source=geo_source,
		  fill_color=factor_cmap('name', palette=Spectral6, factors=df.name.unique()),
          fill_alpha=0.7, line_color="white", line_width=0.5)
	p.multi_line(xs='xs', ys='ys', line_color='black', line_width=0.5, source=geo_source)

	st.bokeh_chart(p, use_container_width=True)


def main():
	"""Visualization of some of the data in the data warehouse"""

	st.sidebar.title("Data visualization UI")
	menu = ["Visualization Map","DHS Surveys"]
	choice = st.sidebar.radio("Menu",menu)

	if choice == "Visualization Map":
		st.title("Find the cluster's nearest health center")

		map()

	elif choice == "DHS Surveys":
		st.title("Surveys available on the DHS platform")
		# st.subheader("Surveys on the DHS platform")
		st.write(data)
		st.sidebar.subheader("Select filter attribute")

		param = st.sidebar.selectbox("Filter results by",["Country","Survey type","Year", "All the above"])
		if param == "Country":
			st.header("Filter results by country")

			countries = data.CountryName.unique()
			option = st.selectbox("Choose a country", countries)
			df = data[data.CountryName == option]
			st.write(df)

			if st.button('Download surveys as CSV'):
			    tmp_download_link = download_link(df, option+'_surveys'+'.csv', 'Click here to download your data!')
			    st.markdown(tmp_download_link, unsafe_allow_html=True)


		elif param == "Survey type":
			st.header("Filter results by survey type")

			SurveyType = data.SurveyType.unique()
			SurveyOption = st.selectbox("Choose a survey type", SurveyType)
			df = data[data.SurveyType == SurveyOption]
			st.write(df)

			if st.button('Download surveys as CSV'):
			    tmp_download_link = download_link(df, SurveyOption+'_surveys'+'.csv', 'Click here to download your data!')
			    st.markdown(tmp_download_link, unsafe_allow_html=True)

		elif param == "Year":
			st.header("Filter results by Year range")

			SurveyYear = data.SurveyYear.unique().tolist()
			SurveyYear.sort()
			a,b = st.slider('Select the year range', min(SurveyYear), max(SurveyYear), (min(SurveyYear), min(SurveyYear)+10), 1)

			if a == b:
				st.write('Results for the year:', a)
				df = data[data.SurveyYear == a]
				st.write(df)

				if st.button('Download surveys as CSV'):
				    tmp_download_link = download_link(df, str(a)+'_surveys'+'.csv', 'Click here to download your data!')
				    st.markdown(tmp_download_link, unsafe_allow_html=True)
			else:
				st.write('Results for year range:', a, 'to', b)
				df = data[(data.SurveyYear >= a) & (data.SurveyYear <= b)]
				st.write(df)

				if st.button('Download surveys as CSV'):
				    tmp_download_link = download_link(df, str(a)+'_to_'+str(b)+'_surveys'+'.csv', 'Click here to download your data!')
				    st.markdown(tmp_download_link, unsafe_allow_html=True)


		elif param == "All the above":
			st.header("Filter results by country, survey type and year")
			col1, col2, col3 = st.beta_columns(3)

			countries = data.CountryName.unique()
			option1 = col1.selectbox("Choose country", countries)

			SurveyType = data.SurveyType.unique()
			option2 = col2.selectbox("Choose survey type", SurveyType)

			SurveyYear = data.SurveyYear.unique().tolist()
			SurveyYear.sort()
			a,b = min(SurveyYear), max(SurveyYear)
			option3 = col3.number_input("Enter year", a,b)

			df = data[(data.CountryName == option1) & (data.SurveyType == option2) & (data.SurveyYear == option3)]
			st.write(df)

			if st.button('Download survey information as CSV'):
				tmp_download_link = download_link(df, option1+'_'+option2+'_'+str(option3)+'_surveys'+'.csv', 'Click here to download your data!')
				st.markdown(tmp_download_link, unsafe_allow_html=True)


data = load_data()

if __name__ == '__main__':
	main()

st.sidebar.markdown('''
### Developed by
[Mugumya Twarik Harouna](https://github.com/twarik)''')

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

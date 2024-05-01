# Import python packages
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write(
    """Orders that need to be filled.
    """
)

# Create a session
cnx = st.connection("snowflake")
session = cnx.session()

# Now you can get the active session
active_session = get_active_session()

# Commented out unnecessary dropdown for selecting ingredients
# my_dataframe = active_session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
# ingredients_list = st.multiselect ('Choose up to 5 ingredients:', my_dataframe)

my_data_frame = active_session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

editable_df = st.data_editor(my_data_frame) 

submitted = st.button('Submit')

if submitted:
    og_dataset = active_session.table("smoothies.public.orders")
    edited_dataset = active_session.create_dataframe(editable_df)
    
    try:
        og_dataset.merge(
            edited_dataset,
            (og_dataset['order_uid'] == edited_dataset['order_uid']),
            [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
        )
        st.success("Order(s) Updated.", icon ="👍")
    except:
        st.write('Something went wrong.')

else:
    st.success('There are no pending orders right now', icon ="👍" )

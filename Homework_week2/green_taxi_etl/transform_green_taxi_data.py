import re 

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def camel_to_snake(camel_str):
    snake_str = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', camel_str)
    return snake_str.lower()

@transformer
def transform(data, *args, **kwargs):
    
    print("Rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data = data[data['trip_distance'] > 0]
    data = data[data['passenger_count'] > 0]
    data.rename(columns=lambda x: camel_to_snake(x), inplace=True)
    print('number of columns:', data['vendor_id'].isin([2]).sum())
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert output['vendor_id'].isin([1,2]).all(), "Invalid value in 'vendor_id' column"
    assert (output['passenger_count'] > 0).all(), "Invalid value in 'passenger_count' column"
    assert (output['trip_distance'] > 0).all(), "Invalid value in 'trip_distance' column"

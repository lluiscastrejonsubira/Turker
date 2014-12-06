import ConfigParser

# Globlals
settings_path ="settings.conf" 

def create_default_settings():
    
    # Create config
    config = ConfigParser.RawConfigParser()
    
    config.add_section('Settings')
    config.set('Settings', 'access_key', 
               'put here your access key')
    config.set('Settings', 'secret_key', 
               'put here your secret key')
    config.set('Settings', 'server_url', 
               'put here your server url')
    config.set('Settings', 'db', 
               'insert db engine for sqlalchemy')
    config.set('Settings', 'images_path', 
               'insert path to the images folder')
    
    # Write settings
    with open(settings_path, 'w') as configfile:
        config.write(configfile)    
        
def create_new_batch(path):
    
    # Create config
    config = ConfigParser.RawConfigParser()
    
    config.add_section('Batch')

    config.set('Batch', 'name',
               'insert name for the batch')
    config.set('Batch', 'sandbox',
               'True to use sandbox, False otherwise')
    config.set('Batch', 'title', 
               'insert task title')
    config.set('Batch', 'description', 
               'insert task description')
    config.set('Batch', 'keywords', 
               'insert one keyword')
    config.set('Batch', 'question', 
               'insert the suburl of your server')
    config.set('Batch', 'amount',
               0.00)
    config.set('Batch', 'duration',
               3600)
    config.set('Batch', 'lifetime',
               604800)
    config.set('Batch', 'auto_approve',
               604800)
    config.set('Batch', 'max_assigs', 
               5)
    config.set('Batch', 'images_per_task', 
               1)
    config.set('Batch', 'height', 
               800)
    config.set('Batch', 'number_of_images', 
               0)
    config.set('Batch', 'image_group',
               'Enter the desired image_group here')
    config.set('Batch', 'batch_tasks',
               'Enter the batch for which to validate tasks')
    config.set('Batch', 'min_approved_amount', 
               100)
    config.set('Batch', 'min_approved_percent', 
               90)
    config.set('Batch', 'country_code', 
               'None')
    
    # Write settings
    with open(path, 'w') as configfile:
        config.write(configfile)    
        
def get_settings(path=settings_path):
    

    config = ConfigParser.RawConfigParser()
    dataset = config.read(path)
    
    if len(dataset) == 0:
        return None
    else:
        return config
    
      

def get_batch_settings(path):

    batch = ConfigParser.RawConfigParser()
    batch.read(path)
    
    return batch
         

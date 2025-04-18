import os
import subprocess

class DltPipeline:
    """
    This is the class to create and handle pipelines
    """
    
    @staticmethod
    def create(data):
        """
        This is the pipeline creation
        """
        file_name = data['pipeline']
    
        file_path = f'../destinations/{file_name}.py'
        file_open_flag = 'x+'
        
        template = DltPipeline.get_template()
        
        if os.path.exists(file_path):
            return 'Pipeline exists already'

        with open(file_path, file_open_flag, encoding='utf-8') as file:
            for field in data.keys():
                template = template\
                    .replace(f'%{field}%', f'"{data[field]}"')
            
            file.write(template)
            
        result = subprocess.run(['python', file_path],
                                check=True,
                                capture_output=True,
                                text=True)
        
        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout)
        print("Standard Error:", result.stderr)
        
        
    @staticmethod
    def get_template():
    
        tplt = ''
        file_path = 'pipeline_templates'
        file_name = 'simple.txt'
        
        with open(f'{file_path}/{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()
            
        return tplt
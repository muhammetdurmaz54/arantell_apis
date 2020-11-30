class IndividualProcessors():
    def __init__(self):
        pass

    def rpm_processor(self,base_dict):
        base_dict['processed'] = int(base_dict['reported'])
        base_dict['z_score'] = self.ship_stats
        return base_dict

    def speed_processor(self,base_dict):
        base_dict['processed'] = int(base_dict['reported'])
        base_dict['z_score'] = self.ship_stats
        return base_dict


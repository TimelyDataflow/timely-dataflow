use progress::Timestamp;
use communication::Data;

use columnar::{Columnar, ColumnarVec};


pub struct BinaryExchangeSender<T, D, RT, RD>
where T:Timestamp+Columnar<RT>,
      D:Data+Columnar<RD>,
      RT: ColumnarVec<T>,
      RD: ColumnarVec<D>
{
    time_columns:       Vec<RT>,                            // columns for time
    data_columns:       Vec<RD>,                            // columns for data

    partition_function: |D|:'static -> u64,                 // function partitioning among outputs

    current:            T,                                  // time we are currently serializing

    header:             MessageHeader,                      // template for message header

    network:            Sender<(MessageHeader, Vec<u8>)>,   // where headers and data go
    buffers:            Receiver<Vec<u8>>,                  // where buffers come from
}

impl<T:Timestamp, D:Data> TargetPort<T, D> for BinaryExchangeSender<T, D>
{
    fn deliver_data(&mut self, time: &T, data: &Vec<D>) {
        if self.current.ne(time) { self.flush(); }
        for &datum in data.iter() {
            let target = (self.partition_function)(datum) % self.data_columns.len() as u64;
            self.data_columns[target].push(datum);
        }
    }

    fn flush(&mut self) {
        let mut writer = MemWriter::from_vec(match self.buffers.try_recv { Some(b) => b, None => Vec::new() })

        self.time_columns.write_to(&mut writer);
        self.data_columns.write_to(&mut writer);

        let buffer = writer.into_inner();

        
    }
}

pub struct BinaryExchangeReceiver<T:Timestamp, D:Data>
{
}

impl<T:Timestamp, D:Data> Iterator<(T,Vec<D>)> for ExchangeReceiver<T, D>
{
    fn next(&mut self) -> Option<(T, Vec<D>)>
    {
        None
    }
}


impl<T:Timestamp, D:Data> BinaryExchangeReceiver<T, D>
{
    fn drain(&mut self)
    {

    }

    pub fn pull_progress(&mut self, consumed: &mut Vec<(T, i64)>, progress: &mut Vec<(T, i64)>)
    {

    }
}

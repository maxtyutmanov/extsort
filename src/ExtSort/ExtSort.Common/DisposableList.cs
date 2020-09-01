using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Common
{
    public class DisposableList<T> : List<T>, IDisposable
        where T: IDisposable
    {
        public void Dispose()
        {
            ForEach(x => x.Dispose());
        }
    }
}

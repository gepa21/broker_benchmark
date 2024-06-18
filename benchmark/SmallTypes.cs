using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmark
{
    [Flags]
    public enum TestComponentModes
    {
        None = 0,
        Producer = 1 << 0,
        Consumer = 1 << 1,
    }
}
